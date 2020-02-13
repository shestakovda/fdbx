package fdbx_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shestakovda/fdbx"
	"github.com/shestakovda/fdbx/models"
	"github.com/stretchr/testify/assert"

	fbs "github.com/google/flatbuffers/go"
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
	defer conn.ClearDB()

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

	assert.True(t, errors.Is(conn.Tx(func(db fdbx.DB) error { return db.Load(nil, rec1, rec3) }), fdbx.ErrRecordNotFound))
	assert.True(t, errors.Is(conn.Tx(func(db fdbx.DB) error { return db.Load(nil, rec3, rec1) }), fdbx.ErrRecordNotFound))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(nil, rec1, rec3) }))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Load(nil, rec2, rec4) }))
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Load(nil, rec4, rec2) }))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
		list, err := db.Select(fdbx.RecordType{ID: TestCollection, New: recordFabric})
		assert.NoError(t, err)
		assert.Len(t, list, 2)

		if string(rec1.ID) < string(rec3.ID) {
			assert.Equal(t, rec1, list[0])
			assert.Equal(t, rec3, list[1])
		} else {
			assert.Equal(t, rec1, list[1])
			assert.Equal(t, rec3, list[0])
		}

		ids, err := db.SelectIDs(TestCollection)
		assert.NoError(t, err)
		assert.Len(t, ids, 2)

		if string(rec1.ID) < string(rec3.ID) {
			assert.Equal(t, rec1.ID, ids[0])
			assert.Equal(t, rec3.ID, ids[1])
		} else {
			assert.Equal(t, rec1.ID, ids[1])
			assert.Equal(t, rec3.ID, ids[0])
		}

		return nil
	}))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Drop(nil, rec1, rec3) }))

	assert.True(t, errors.Is(conn.Tx(func(db fdbx.DB) error { return db.Load(nil, rec2, rec4) }), fdbx.ErrRecordNotFound))
	assert.True(t, errors.Is(conn.Tx(func(db fdbx.DB) error { return db.Load(nil, rec4, rec2) }), fdbx.ErrRecordNotFound))

	assert.Equal(t, rec1, rec2)
	assert.Equal(t, rec3, rec4)
}

func TestSelect(t *testing.T) {
	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())
	defer conn.ClearDB()

	records := make([]fdbx.Record, 10)
	for i := range records {
		records[i] = newTestRecord()
	}

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(nil, records...) }))

	cur, err := conn.Cursor(fdbx.RecordType{ID: TestCollection, New: recordFabric}, fdbx.Page(3))
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
	cur, err = conn.Cursor(fdbx.RecordType{ID: TestCollection, New: recordFabric}, fdbx.Page(3))
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

	assert.NoError(t, cur.Close())

	// ********* REVERSE *********

	cur, err = conn.Cursor(fdbx.RecordType{ID: TestCollection, New: recordFabric}, fdbx.Page(3), fdbx.Reverse())
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	// ********* all in *********

	recc, errc = cur.Select(ctx)

	recs2 := make([]fdbx.Record, 0, 10)
	for rec := range recc {
		recs2 = append(recs2, rec)
	}

	errs = make([]error, 0)
	for err := range errc {
		errs = append(errs, err)
	}

	assert.Len(t, errs, 0)
	assert.Len(t, recs2, 10)
	assert.True(t, cur.Empty())
	assert.NoError(t, cur.Close())

	// ********* steps *********

	recl2 := make([]fdbx.Record, 0, 10)
	rect2 := make([]fdbx.Record, 0, 10)

	// page size = 3
	cur, err = conn.Cursor(fdbx.RecordType{ID: TestCollection, New: recordFabric}, fdbx.Page(3), fdbx.Reverse())
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	// pos: 0 -> (load 3) -> 3
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl2, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl2, 3)
	assert.False(t, cur.Empty())
	rect2 = append(rect2, recl2...)

	// pos: 3 -> (skip 3) -> 6 -> (load 3) -> 9
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl2, err = cur.Next(db, 1); return err }))
	assert.Len(t, recl2, 3)
	assert.False(t, cur.Empty())

	// pos: 9 -> (skip -6) -> 3 -> (load 3) -> 6
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl2, err = cur.Prev(db, 1); return err }))
	assert.Len(t, recl2, 3)
	assert.False(t, cur.Empty())
	rect2 = append(rect2, recl2...)

	// pos: 6 -> (load 3) -> 9
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl2, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl2, 3)
	assert.False(t, cur.Empty())
	rect2 = append(rect2, recl2...)

	// pos: 9 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl2, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl2, 1)
	assert.True(t, cur.Empty())
	rect2 = append(rect2, recl2...)

	// important!
	assert.Equal(t, recs2, rect2)

	// pos: 10 -> (skip -3) -> 7 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl2, err = cur.Prev(db, 0); return err }))
	assert.Len(t, recl2, 3)
	assert.False(t, cur.Empty())

	// pos: 10 -> (skip -3) -> 7 -> (load 3) -> 10
	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { recl2, err = cur.Next(db, 0); return err }))
	assert.Len(t, recl2, 0)
	assert.True(t, cur.Empty())

	// very important!
	fdbx.ReverseList(recs2)
	assert.Equal(t, recs, recs2)
}

func TestIndex(t *testing.T) {
	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())
	defer conn.ClearDB()

	records := make([]fdbx.Record, 10)
	for i := range records {
		records[i] = newTestRecord()
	}

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(nil, records...) }))

	cur, err := conn.Cursor(fdbx.RecordType{ID: TestIndexName, New: recordFabric}, fdbx.Page(3))
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
	filter := fdbx.Filter(func(fdbx.Record) (bool, error) { return true, nil })
	cur, err = conn.Cursor(fdbx.RecordType{ID: TestIndexName, New: recordFabric}, fdbx.Page(3), filter)
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

	assert.Len(t, cur.FdbxID(), 32)
	cur, err = conn.LoadCursor(cur.FdbxID(), recordFabric, fdbx.Page(3))
	assert.NoError(t, err)
	assert.NotNil(t, cur)

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

		list, err := db.Select(fdbx.RecordType{ID: TestIndexName, New: recordFabric}, fdbx.From([]byte(max)))
		assert.NoError(t, err)
		assert.Len(t, list, 1)
		assert.Equal(t, rec, list[0])
		return nil
	}))

	// ********* clear *********

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.ClearIndex(new(testRecord).FdbxIndex) }))

	cur, err = conn.Cursor(fdbx.RecordType{ID: TestIndexName, New: recordFabric}, fdbx.Page(3))
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	recc, errc = cur.Select(ctx)

	recs = make([]fdbx.Record, 0, 10)
	for rec := range recc {
		recs = append(recs, rec)
	}

	errs = make([]error, 0)
	for err := range errc {
		errs = append(errs, err)
	}

	assert.Len(t, errs, 0)
	assert.Len(t, recs, 0)
	assert.True(t, cur.Empty())
	assert.NoError(t, cur.Close())
}

func TestLongValuesCollection(t *testing.T) {
	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())
	defer conn.ClearDB()

	records := make([]fdbx.Record, 3)
	for i := range records {
		records[i] = newTestRecord()
	}

	guid := records[1].(*testRecord).Data
	records[1].(*testRecord).Data = bytes.Repeat(guid, 10000)

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(nil, records...) }))

	// ********* filter *********

	cur, err := conn.Cursor(
		fdbx.RecordType{ID: TestCollection, New: recordFabric},
		fdbx.Page(3),
		fdbx.Filter(func(rec fdbx.Record) (bool, error) {
			if len(rec.(*testRecord).Data) > 1000 {
				return false, nil
			}
			return true, nil
		}),
	)
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	defer func() { assert.NoError(t, cur.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recc, errc := cur.Select(ctx)

	recs := make([]fdbx.Record, 0, 2)
	for rec := range recc {
		recs = append(recs, rec)
	}

	errs := make([]error, 0)
	for err := range errc {
		errs = append(errs, err)
	}

	assert.Len(t, errs, 0)
	assert.Len(t, recs, 2)
	assert.True(t, cur.Empty())
	assert.NoError(t, cur.Close())
}

func TestLongValuesIndex(t *testing.T) {
	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())
	defer conn.ClearDB()

	records := make([]fdbx.Record, 3)
	for i := range records {
		records[i] = newTestRecord()
	}

	guid := records[1].(*testRecord).Data
	records[1].(*testRecord).Data = bytes.Repeat(guid, 10000)

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(nil, records...) }))

	// ********* filter *********

	cur, err := conn.Cursor(
		fdbx.RecordType{ID: TestIndexName, New: recordFabric},
		fdbx.Page(3),
		fdbx.Filter(func(rec fdbx.Record) (bool, error) {
			if len(rec.(*testRecord).Data) > 1000 {
				return false, nil
			}
			return true, nil
		}),
	)
	assert.NoError(t, err)
	assert.NotNil(t, cur)
	assert.False(t, cur.Empty())

	defer func() { assert.NoError(t, cur.Close()) }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	recc, errc := cur.Select(ctx)

	recs := make([]fdbx.Record, 0, 2)
	for rec := range recc {
		recs = append(recs, rec)
	}

	errs := make([]error, 0)
	for err := range errc {
		errs = append(errs, err)
	}

	assert.Len(t, errs, 0)
	assert.Len(t, recs, 2)
	assert.True(t, cur.Empty())
}

func TestQueue(t *testing.T) {
	const recCount = 3

	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())
	defer conn.ClearDB()

	records := make([]fdbx.Record, recCount)
	for i := range records {
		records[i] = newTestRecord()
	}

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(nil, records...) }))

	notFound := "deleted!"
	notFoundHandler := fdbx.OnNotFound(func(rec fdbx.Record) error {
		assert.Equal(t, notFound, rec.FdbxID())
		return nil
	})

	queue, err := conn.Queue(fdbx.RecordType{ID: TestQueueType, New: recordFabric}, "memberID", notFoundHandler)
	assert.NoError(t, err)
	assert.NotNil(t, queue)

	// to accelerate tasks
	fdbx.PunchSize = 10 * time.Millisecond

	var wg sync.WaitGroup
	wg.Add(1)

	// subscribe
	go func() {
		defer wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()

		recc, errc := queue.Sub(ctx)

		recs := make([]fdbx.Record, 0, recCount)
		for rec := range recc {
			recs = append(recs, rec)
		}

		errs := make([]error, 0, 1)
		for err := range errc {
			errs = append(errs, err)
		}

		assert.Len(t, recs, recCount)
		assert.Len(t, errs, 1)
		assert.True(t, errors.Is(errs[0], context.DeadlineExceeded))
	}()

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
		return queue.Pub(db, time.Now().Add(fdbx.PunchSize), notFound)
	}))

	for i := 0; i < recCount; i++ {
		time.Sleep(fdbx.PunchSize)

		assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
			return queue.Pub(db, time.Now().Add(fdbx.PunchSize), records[i].FdbxID())
		}))
	}

	wg.Wait()

	lost, err := queue.GetLost(0, nil)
	assert.NoError(t, err)
	assert.Len(t, lost, recCount)

	wcnt, lcnt, err := queue.Stat()
	assert.NoError(t, err)
	assert.Equal(t, 0, wcnt)
	assert.Equal(t, recCount+1, lcnt)

	ack := make([]string, len(lost))
	for i := range lost {
		ack[i] = lost[i].FdbxID()
	}

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return queue.Ack(db, ack[1]) }))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error {
		res, err := queue.Status(db, ack...)
		assert.Len(t, res, recCount)
		assert.Equal(t, map[string]fdbx.TaskStatus{
			ack[0]: fdbx.StatusUnconfirmed,
			ack[1]: fdbx.StatusConfirmed,
			ack[2]: fdbx.StatusUnconfirmed,
		}, res)
		return err
	}))

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return queue.Ack(db, ack...) }))

	lost, err = queue.GetLost(0, nil)
	assert.NoError(t, err)
	assert.Len(t, lost, 0)
}

func TestIntersect(t *testing.T) {
	var ids []string
	var crsName, crsNum fdbx.CursorID

	ctx := context.Background()

	rtpName := fdbx.RecordType{ID: TestIndexName, New: recordFabric}
	rtpNum := fdbx.RecordType{ID: TestIndexNumber, New: recordFabric}

	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	assert.NoError(t, conn.ClearDB())
	defer conn.ClearDB()

	recs := makePack(999)
	arec := newTestRecord()
	arec.Name = "42"
	arec.Number = 42
	recs = append(recs, arec)

	assert.NoError(t, conn.Tx(func(db fdbx.DB) error { return db.Save(nil, recs...) }))

	query := fdbx.Query([]byte("42"))

	crsName, err = conn.CursorID(rtpName, query)
	assert.NoError(t, err)

	crsNum, err = conn.CursorID(rtpNum, query)
	assert.NoError(t, err)

	crs := []fdbx.CursorID{crsName, crsNum}

	ids, err = fdbx.Intersect(ctx, crs, 0)
	assert.NoError(t, err)
	assert.True(t, len(ids) >= 1)
}

func makePack(cnt int) []fdbx.Record {
	recs := make([]fdbx.Record, cnt)

	for i := range recs {
		recs[i] = newTestRecord()
	}

	return recs
}

func benchmarkSave(b *testing.B, cnt int) {
	var err error
	b.StopTimer()

	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(b, err)
	assert.NotNil(b, conn)
	assert.NoError(b, conn.ClearDB())
	defer conn.ClearDB()

	recs := makePack(cnt)

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		if err = conn.Tx(func(db fdbx.DB) error { return db.Save(nil, recs...) }); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkLoad(b *testing.B, page uint, cnt int) {
	var err error
	var cur fdbx.Cursor

	rtp := fdbx.RecordType{ID: TestCollection, New: recordFabric}

	b.StopTimer()

	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(b, err)
	assert.NotNil(b, conn)
	assert.NoError(b, conn.ClearDB())
	defer conn.ClearDB()

	for i := 0; i < cnt; i++ {
		recs := makePack(int(page))

		if err = conn.Tx(func(db fdbx.DB) error { return db.Save(nil, recs...) }); err != nil {
			b.Fatal(err)
		}
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {

		if cur, err = conn.Cursor(rtp, fdbx.Page(page)); err != nil {
			return
		}

		recs, errs := cur.Select(context.Background())

		for rec := range recs {
			if rec == nil || rec.FdbxID() == "" {
				b.Fatal("no record!")
			}
		}

		for err := range errs {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func benchmarkLoadIDs(b *testing.B, page uint, cnt int) {
	var err error

	// rtp := fdbx.RecordType{ID: TestCollection, New: recordFabric}

	b.StopTimer()

	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(b, err)
	assert.NotNil(b, conn)
	assert.NoError(b, conn.ClearDB())
	defer conn.ClearDB()

	for i := 0; i < cnt; i++ {
		recs := makePack(int(page))

		if err = conn.Tx(func(db fdbx.DB) error { return db.Save(nil, recs...) }); err != nil {
			b.Fatal(err)
		}
	}

	b.StartTimer()

	var ids []string

	for i := 0; i < b.N; i++ {

		if err = conn.Tx(func(db fdbx.DB) (exp error) {
			ids, exp = db.SelectIDs(TestIndexName)
			return
		}); err != nil {
			b.Fatal(err)
		}

		if len(ids) != int(page)*cnt {
			b.Fatal("bad len")
		}
	}
}

func BenchmarkIntersect(b *testing.B) {
	var ids []string
	var crsName, crsNum fdbx.CursorID

	cnt := 10
	page := 1000
	ctx := context.Background()

	rtpName := fdbx.RecordType{ID: TestIndexName, New: recordFabric}
	rtpNum := fdbx.RecordType{ID: TestIndexNumber, New: recordFabric}

	conn, err := fdbx.NewConn(TestDatabase, TestVersion)
	assert.NoError(b, err)
	assert.NotNil(b, conn)
	assert.NoError(b, conn.ClearDB())
	defer conn.ClearDB()

	for i := 0; i < cnt; i++ {
		recs := makePack(page - 1)

		arec := newTestRecord()
		arec.Name = "4"
		arec.Number = 4

		recs = append(recs, arec)

		if err = conn.Tx(func(db fdbx.DB) error { return db.Save(nil, recs...) }); err != nil {
			b.Fatal(err)
		}
	}

	if crsName, err = conn.CursorID(rtpName); err != nil {
		b.Fatal(err)
	}

	if crsNum, err = conn.CursorID(rtpNum); err != nil {
		b.Fatal(err)
	}

	crs := []fdbx.CursorID{crsName, crsNum}

	if ids, err = fdbx.Intersect(ctx, crs, 0); err != nil {
		b.Fatal(err)
	}

	total := len(ids)

	b.Run(fmt.Sprintf("%d-pairs", total), func(b *testing.B) {
		for i := 0; i < b.N; i++ {

			if crsName, err = conn.CursorID(rtpName); err != nil {
				b.Fatal(err)
			}

			if crsNum, err = conn.CursorID(rtpNum); err != nil {
				b.Fatal(err)
			}

			crs := []fdbx.CursorID{crsName, crsNum}

			if ids, err = fdbx.Intersect(ctx, crs, 0); err != nil {
				b.Fatal(err)
			}

			if len(ids) != total {
				b.Fatal("incorrect result")
			}
		}
	})
}

func BenchmarkSave1(b *testing.B)    { benchmarkSave(b, 1) }
func BenchmarkSave10(b *testing.B)   { benchmarkSave(b, 10) }
func BenchmarkSave100(b *testing.B)  { benchmarkSave(b, 100) }
func BenchmarkSave1000(b *testing.B) { benchmarkSave(b, 1000) }

func BenchmarkLoad1_1000(b *testing.B)   { benchmarkLoad(b, 1000, 1) }
func BenchmarkLoad10_1000(b *testing.B)  { benchmarkLoad(b, 1000, 10) }
func BenchmarkLoad100_1000(b *testing.B) { benchmarkLoad(b, 1000, 100) }

func BenchmarkLoadID1_1000(b *testing.B)   { benchmarkLoadIDs(b, 1000, 1) }
func BenchmarkLoadID10_1000(b *testing.B)  { benchmarkLoadIDs(b, 1000, 10) }
func BenchmarkLoadID100_1000(b *testing.B) { benchmarkLoadIDs(b, 1000, 100) }

func recordFabric(ver uint8, id string) (fdbx.Record, error) { return &testRecord{ID: id}, nil }

func newTestRecord() *testRecord {
	uid := uuid.New()
	str := uid.String()
	nop := []byte{0, 0, 0, 0, 0, 0}
	num := binary.BigEndian.Uint64(append(nop, uid[:2]...))
	flt := float64(binary.BigEndian.Uint64(append(nop, uid[2:4]...)))

	return &testRecord{
		ID:      uid.String(),
		Name:    str,
		Number:  num,
		Decimal: flt,
		Logic:   flt > float64(num),
		Data:    uid[:],
		Strs:    []string{str, str, str},
	}
}

type testRecord struct {
	ID      string   `json:"-"`
	Name    string   `json:"name"`
	Number  uint64   `json:"number"`
	Decimal float64  `json:"decimal"`
	Logic   bool     `json:"logic"`
	Data    []byte   `json:"data"`
	Strs    []string `json:"strs"`

	notFound      bool
	raiseNotFound bool
}

func (r *testRecord) FdbxID() string { return r.ID }
func (r *testRecord) FdbxType() fdbx.RecordType {
	return fdbx.RecordType{ID: TestCollection, New: recordFabric}
}
func (r *testRecord) FdbxIndex(idx fdbx.Indexer) error {
	idx.Grow(3)
	idx.Index(TestIndexName, []byte(r.Name))
	idx.Index(TestIndexNumber, []byte(fmt.Sprintf("%d", r.Number)))
	idx.Index(TestIndexNumber, []byte(fmt.Sprintf("%d", int(r.Decimal))))
	return nil
}
func (r *testRecord) FdbxMarshal() ([]byte, error) {
	size := len(r.Name) + 5 + 8 + 8 + 1 + len(r.Data) + 5 + 5*len(r.Strs)
	for i := range r.Strs {
		size += 5 + len(r.Strs[i])
	}

	buf := fbs.NewBuilder(size)

	nameOffset := buf.CreateString(r.Name)
	dataOffset := buf.CreateByteVector(r.Data)

	// Strs
	strsCount := len(r.Strs)
	var strsArr fbs.UOffsetT
	if strsCount != 0 {
		strs := make([]fbs.UOffsetT, strsCount)
		for i := range r.Strs {
			strs[strsCount-i-1] = buf.CreateString(r.Strs[i])
		}
		models.TestRecordStartStringsVector(buf, strsCount)
		for i := range strs {
			buf.PrependUOffsetT(strs[i])
		}
		strsArr = buf.EndVector(strsCount)
	}

	models.TestRecordStart(buf)
	models.TestRecordAddName(buf, nameOffset)
	models.TestRecordAddData(buf, dataOffset)
	models.TestRecordAddFloat(buf, r.Decimal)
	models.TestRecordAddLogic(buf, r.Logic)
	models.TestRecordAddNumber(buf, r.Number)
	models.TestRecordAddStrings(buf, strsArr)
	buf.Finish(models.TestRecordEnd(buf))

	return buf.FinishedBytes(), nil
}
func (r *testRecord) FdbxUnmarshal(buf []byte) error {
	model := models.GetRootAsTestRecord(buf, 0)

	r.Decimal = model.Float()
	r.Logic = model.Logic()
	r.Number = model.Number()
	r.Name = fdbx.B2S(model.Name())
	r.Data = model.DataBytes()

	// Strs
	if model.StringsLength() != 0 {
		r.Strs = make([]string, model.StringsLength())
		for i := range r.Strs {
			r.Strs[i] = fdbx.B2S(model.Strings(i))
		}
	}

	return nil
}
