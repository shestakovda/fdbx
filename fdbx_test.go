package fdbx_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/shestakovda/fdbx"
	"github.com/stretchr/testify/assert"
)

// current test settings
var (
	TestVersion     = fdbx.ConnVersion610
	TestDatabase    = uint16(0x0102)
	TestCollection  = uint16(0x0304)
	TestQueueType   = uint16(0x0506)
	TestIndexName   = uint16(0x0708)
	TestIndexNumber = uint16(0x0910)
)

func TestCrud(t *testing.T) {
	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())

	rec1 := newTestRecord()
	rec2 := &testRecord{ID: rec1.ID}

	rec3 := newTestRecord()
	rec4 := &testRecord{ID: rec3.ID}

	// ******** Key/Value ********

	uid := uuid.New()
	key := uid[:8]
	val := uid[8:16]

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
		v, e := db.Get(TestCollection, key)
		assert.Empty(t, v)
		return e
	}))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Set(TestCollection, key, val) }))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
		v, e := db.Get(TestCollection, key)
		assert.Equal(t, val, v)
		return e
	}))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Del(TestCollection, key) }))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
		v, e := db.Get(TestCollection, key)
		assert.Empty(t, v)
		return e
	}))

	// ******** Record ********

	assert.True(t, errors.Is(conn.Tx(func(db fdbx.DB) error { return db.Load(rec1, rec3) }), fdbx.ErrRecordNotFound))
	assert.True(t, errors.Is(conn.Tx(func(db fdbx.DB) error { return db.Load(rec3, rec1) }), fdbx.ErrRecordNotFound))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(rec1, rec3) }))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Load(rec2, rec4) }))
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Load(rec4, rec2) }))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
		list, err := db.Select(TestCollection, recordFabric)
		assert.NoError(t, err)
		assert.Len(t, list, 2)

		if string(rec1.ID) < string(rec3.ID) {
			assert.Equal(t, rec1, list[0])
			assert.Equal(t, rec3, list[1])
		} else {
			assert.Equal(t, rec1, list[1])
			assert.Equal(t, rec3, list[0])
		}

		return err
	}))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Drop(rec1, rec3) }))

	assert.True(t, errors.Is(conn.Tx(func(db fdbx.DB) error { return db.Load(rec2, rec4) }), fdbx.ErrRecordNotFound))
	assert.True(t, errors.Is(conn.Tx(func(db fdbx.DB) error { return db.Load(rec4, rec2) }), fdbx.ErrRecordNotFound))

	assert.Equal(t, rec1, rec2)
	assert.Equal(t, rec3, rec4)
}

func TestSelect(t *testing.T) {
	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())

	records := make([]fdbx.Record, 10)
	for i := range records {
		records[i] = newTestRecord()
	}

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(records...) }))

	cur, err := conn.Cursor(TestCollection, recordFabric, nil, 3)
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	defer func() { assert.NoError(t, cur.Close()) }()

	// ********* all in *********

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recc, errc := cur.Select(ctx)

	recs := make([]fdbx.Record, 0, 10)
	for rec := range recc {
		recs = append(recs, rec)
	}

	errs := make([]error, 0)
	for err := range errc {
		errs = append(errs, err)
	}

	assert.Len(t, errs, 0)
	assert.Len(t, recs, 10)
	assert.True(t, cur.Empty())
	assert.NoError(t, cur.Close())

	// ********* steps *********

	recl := make([]fdbx.Record, 0, 10)
	rect := make([]fdbx.Record, 0, 10)

	// page size = 3
	cur, err = conn.Cursor(TestCollection, recordFabric, nil, 3)
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	// pos: 0 -> (load 3) -> 3
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())
	rect = append(rect, recl...)

	// pos: 3 -> (skip 3) -> 6 -> (load 3) -> 9
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 1); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())

	// pos: 9 -> (skip -6) -> 3 -> (load 3) -> 6
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Prev(db, 1); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())
	rect = append(rect, recl...)

	// pos: 6 -> (load 3) -> 9
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())
	rect = append(rect, recl...)

	// pos: 9 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl, 1)
	assert.True(t, cur.Empty())
	rect = append(rect, recl...)

	// important!
	assert.Equal(t, recs, rect)

	// pos: 10 -> (skip -3) -> 7 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Prev(db, 0); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())

	// pos: 10 -> (skip -3) -> 7 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl, 0)
	assert.True(t, cur.Empty())
}

func TestIndex(t *testing.T) {
	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())

	records := make([]fdbx.Record, 10)
	for i := range records {
		records[i] = newTestRecord()
	}

	conn.RegisterIndex(TestCollection, testRecordIdx)

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(records...) }))

	cur, err := conn.Cursor(TestIndexName, recordFabric, nil, 3)
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	defer func() { assert.NoError(t, cur.Close()) }()

	// ********* all in *********

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	recc, errc := cur.Select(ctx)

	recs := make([]fdbx.Record, 0, 10)
	for rec := range recc {
		recs = append(recs, rec)
	}

	errs := make([]error, 0)
	for err := range errc {
		errs = append(errs, err)
	}

	assert.Len(t, errs, 0)
	assert.Len(t, recs, 10)
	assert.True(t, cur.Empty())
	assert.NoError(t, cur.Close())

	// ********* steps *********

	recl := make([]fdbx.Record, 0, 10)
	rect := make([]fdbx.Record, 0, 10)

	// page size = 3
	cur, err = conn.Cursor(TestIndexName, recordFabric, nil, 3)
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	// pos: 0 -> (load 3) -> 3
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())
	rect = append(rect, recl...)

	// pos: 3 -> (skip 3) -> 6 -> (load 3) -> 9
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 1); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())

	// pos: 9 -> (skip -6) -> 3 -> (load 3) -> 6
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Prev(db, 1); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())
	rect = append(rect, recl...)

	// pos: 6 -> (load 3) -> 9
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())
	rect = append(rect, recl...)

	// pos: 9 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl, 1)
	assert.True(t, cur.Empty())
	rect = append(rect, recl...)

	// important!
	assert.Equal(t, recs, rect)

	// pos: 10 -> (skip -3) -> 7 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Prev(db, 0); return err }))
	assert.Len(t, recl, 3)
	assert.False(t, cur.Empty())

	// pos: 10 -> (skip -3) -> 7 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl, 0)
	assert.True(t, cur.Empty())

	// ********* byValue *********

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
		max := ""
		rec := new(testRecord)

		for i := range records {
			if records[i].(*testRecord).Name > max {
				max = records[i].(*testRecord).Name
				rec = records[i].(*testRecord)
			}
		}

		list, err := db.Select(TestIndexName, recordFabric, fdbx.From([]byte(max)))
		assert.NoError(t, err)
		assert.Len(t, list, 1)
		assert.Equal(t, rec, list[0])
		return nil
	}))
}

func TestQueue(t *testing.T) {
	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())

	records := make([]fdbx.Record, 3)
	for i := range records {
		records[i] = newTestRecord()
	}

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(records...) }))

	queue, err := conn.Queue(TestQueueType, recordFabric, []byte("memberID"))
	assert.NoError(t, err)
	assert.NotNil(t, queue)

	// to accelerate tasks
	fdbx.PunchSize = 100 * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(1)

	// subscribe
	go func() {
		defer wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		recc, errc := queue.Sub(ctx)

		recs := make([]fdbx.Record, 0, 3)
		for rec := range recc {
			recs = append(recs, rec)

			assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return queue.Ack(db, rec) }))
		}

		errs := make([]error, 0, 3)
		for err := range errc {
			errs = append(errs, err)
		}

		assert.Len(t, recs, 3)
		assert.Len(t, errs, 1)
		assert.True(t, errors.Is(errs[0], context.DeadlineExceeded))
	}()

	for i := 0; i < 3; i++ {
		time.Sleep(2 * fdbx.PunchSize)

		assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
			return queue.Pub(db, records[i], time.Now().Add(2*fdbx.PunchSize))
		}))
	}

	wg.Wait()
}

func recordFabric(id []byte) (fdbx.Record, error) { return &testRecord{ID: id}, nil }

func newTestRecord() *testRecord {
	uid := uuid.New()
	str := uid.String()
	nop := []byte{0, 0, 0, 0, 0, 0}
	num := binary.BigEndian.Uint64(append(nop, uid[:2]...))
	flt := float64(binary.BigEndian.Uint64(append(nop, uid[2:4]...)))

	return &testRecord{
		ID:      uid[:],
		Name:    str,
		Number:  num,
		Decimal: flt,
		Logic:   flt > float64(num),
		Data:    uid[:],
		Strs:    []string{str, str, str},
	}
}

func testRecordIdx(idx fdbx.Indexer, buf []byte) (err error) {
	rec := new(testRecord)

	if err = rec.FdbxUnmarshal(buf); err != nil {
		return
	}

	idx.Index(TestIndexName, []byte(rec.Name))
	idx.Index(TestIndexNumber, []byte(fmt.Sprintf("%d", rec.Number)))
	idx.Index(TestIndexNumber, []byte(fmt.Sprintf("%d", int(rec.Decimal))))
	return nil
}

type testRecord struct {
	ID      []byte   `json:"-"`
	Name    string   `json:"name"`
	Number  uint64   `json:"number"`
	Decimal float64  `json:"decimal"`
	Logic   bool     `json:"logic"`
	Data    []byte   `json:"data"`
	Strs    []string `json:"strs"`
}

func (r *testRecord) FdbxID() []byte               { return r.ID }
func (r *testRecord) FdbxType() uint16             { return TestCollection }
func (r *testRecord) FdbxMarshal() ([]byte, error) { return json.Marshal(r) }
func (r *testRecord) FdbxUnmarshal(b []byte) error { return json.Unmarshal(b, r) }
