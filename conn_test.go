package fdbx_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/google/uuid"
	"github.com/shestakovda/fdbx"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	const db = 1
	const skey = "test key"
	const skey2 = "test key 2"
	const skey3 = "test key 3"
	const skey4 = "test key 4"
	const ctype = 2
	const qtype = 3

	var data []byte
	var tkey = []byte(skey)
	var tdata = []byte("test data")
	var tdata2 = []byte("test data 2")
	var tdata3 = []byte("test data 3")
	var tdata4 = []byte("test data 4")
	var terr = errors.New("test err")

	c1, err := fdbx.NewConn(db, fdbx.ConnVersion610)
	assert.NoError(t, err)
	assert.NotNil(t, c1)

	// ************ Clear All ************

	assert.NoError(t, c1.ClearDB())

	// ************ Key ************

	_, err = c1.Key(0, nil)
	assert.True(t, errors.Is(err, fdbx.ErrEmptyID))

	key, err := c1.Key(ctype, tkey)
	assert.NoError(t, err)
	assert.Equal(t, fdb.Key(append([]byte{0, 1, 0, 2}, tkey...)), key)

	// ************ MKey ************

	_, err = c1.MKey(nil)
	assert.True(t, errors.Is(err, fdbx.ErrNullModel))

	key, err = c1.MKey(&testModel{key: skey, ctype: ctype})
	assert.NoError(t, err)
	assert.Equal(t, fdb.Key(append([]byte{0, 1, 0, 2}, tkey...)), key)

	// ************ DB.Set ************

	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Set(0, nil, nil) })
	assert.True(t, errors.Is(err, fdbx.ErrEmptyID))

	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Set(ctype, tkey, tdata) })
	assert.NoError(t, err)

	// ************ DB.Get ************

	err = c1.Tx(func(db fdbx.DB) (e error) { _, e = db.Get(0, nil); return e })
	assert.True(t, errors.Is(err, fdbx.ErrEmptyID))

	err = c1.Tx(func(db fdbx.DB) (e error) { data, e = db.Get(ctype, tkey); return e })
	assert.NoError(t, err)
	assert.Equal(t, tdata, data)

	// ************ DB.Del ************

	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Del(0, nil) })
	assert.True(t, errors.Is(err, fdbx.ErrEmptyID))

	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Del(ctype, tkey) })
	assert.NoError(t, err)

	// ************ DB.Save ************

	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Save(nil) })
	assert.True(t, errors.Is(err, fdbx.ErrNullModel))

	m := &testModel{key: skey, ctype: ctype, err: terr}
	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Save(m) })
	assert.True(t, errors.Is(err, terr))

	// ************ DB.Load ************

	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Load(nil) })
	assert.True(t, errors.Is(err, fdbx.ErrNullModel))

	// ************ DB.Save/DB.Load ************

	m1 := &testModel{key: skey, ctype: ctype, data: tdata}
	m2 := &testModel{key: skey2, ctype: ctype, data: tdata2}
	m5 := &testModel{key: skey, ctype: ctype}
	m6 := &testModel{key: skey2, ctype: ctype}

	k1, err := c1.MKey(m1)
	assert.NoError(t, err)
	k2, err := c1.MKey(m2)
	assert.NoError(t, err)
	k3, err := c1.MKey(m5)
	assert.NoError(t, err)
	k4, err := c1.MKey(m6)
	assert.NoError(t, err)
	assert.Equal(t, k1, k3)
	assert.Equal(t, k2, k4)

	fdbx.GZipSize = 0
	fdbx.ChunkSize = 2

	assert.NoError(t, c1.Tx(func(db fdbx.DB) (e error) {
		if e = db.Save(m1, m2); e != nil {
			return
		}

		return db.Load(m5, m6)
	}))

	assert.Equal(t, m1.Dump(), m5.Dump())
	assert.Equal(t, m2.Dump(), m6.Dump())

	// ************ Queue Pub/Sub ************

	fab := func(id []byte) fdbx.Model { return &testModel{key: string(id), ctype: ctype} }
	queue := c1.Queue(qtype, fab)

	m3 := &testModel{key: skey3, ctype: ctype, data: tdata3}
	m4 := &testModel{key: skey4, ctype: ctype, data: tdata4}

	// publish 3 tasks
	assert.NoError(t, c1.Tx(func(db fdbx.DB) (e error) {
		if e = db.Save(m3, m4); e != nil {
			return
		}

		assert.True(t, errors.Is(queue.Pub(nil, m1, time.Now()), fdbx.ErrNullDB))
		assert.True(t, errors.Is(queue.Pub(db, nil, time.Now()), fdbx.ErrNullModel))

		if e = queue.Pub(db, m1, time.Now()); e != nil {
			return
		}

		time.Sleep(time.Millisecond)
		if e = queue.Pub(db, m2, time.Now()); e != nil {
			return
		}

		time.Sleep(time.Millisecond)
		if e = queue.Pub(db, m3, time.Now()); e != nil {
			return
		}

		return nil
	}))

	var wg sync.WaitGroup
	wg.Add(2)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	wrk := func(s string) {
		defer wg.Done()
		mods, err := queue.SubList(ctx, 2)
		assert.NoError(t, err)
		assert.Len(t, mods, 2)
		assert.NoError(t, c1.Tx(func(db fdbx.DB) error {
			assert.True(t, errors.Is(queue.Ack(nil, nil), fdbx.ErrNullDB))
			assert.True(t, errors.Is(queue.Ack(db, nil), fdbx.ErrNullModel))

			for i := range mods {
				assert.NoError(t, queue.Ack(db, mods[i]))
			}
			return nil
		}))
	}

	// 1 worker get 2 tasks and exit
	// 2 worker get 1 task and wait
	go wrk(skey2)
	go wrk(skey4)

	// publish 4 task
	assert.NoError(t, c1.Tx(func(db fdbx.DB) (e error) { return queue.Pub(db, m4, time.Now()) }))

	// wait all
	wg.Wait()

	wg.Add(1)
	go func() {
		defer wg.Done()
		m, err := queue.SubOne(ctx)
		assert.NoError(t, err)
		assert.Equal(t, skey, string(m.ID()))
		assert.Equal(t, string(tdata), string(m.Dump()))
		assert.NoError(t, c1.Tx(func(db fdbx.DB) (e error) { return queue.Ack(db, m) }))
	}()

	// publish 1 task
	assert.NoError(t, c1.Tx(func(db fdbx.DB) (e error) { return queue.Pub(db, m1, time.Now()) }))

	// wait all
	wg.Wait()

	modc, errc := queue.Sub(ctx)

	// publish 1 task
	assert.NoError(t, c1.Tx(func(db fdbx.DB) (e error) {
		if e = queue.Pub(db, m1, time.Now()); e != nil {
			return
		}

		time.Sleep(time.Millisecond)
		if e = queue.Pub(db, m2, time.Now()); e != nil {
			return
		}

		time.Sleep(time.Millisecond)
		return queue.Pub(db, m3, time.Now())
	}))

	mods := make([]fdbx.Model, 0, 3)
	errs := make([]error, 0, 3)

	for m := range modc {
		mods = append(mods, m)
	}

	for e := range errc {
		errs = append(errs, e)
	}

	assert.Len(t, mods, 3)
	assert.Len(t, errs, 1)
	assert.True(t, errors.Is(errs[0], context.DeadlineExceeded))
	assert.Equal(t, skey, string(mods[0].ID()))
	assert.Equal(t, skey2, string(mods[1].ID()))
	assert.Equal(t, skey3, string(mods[2].ID()))

	// ************ DB.Select ************

	var list []fdbx.Model

	predicat := func(buf []byte) (bool, error) {
		if string(buf) == string(tdata2) {
			return false, nil
		}
		return true, nil
	}

	assert.NoError(t, c1.Tx(func(db fdbx.DB) (e error) {
		list, e = db.Select(
			ctype, fab,
			fdbx.Limit(3),
			fdbx.PrefixLen(4),
			fdbx.Filter(predicat),
			fdbx.GTE([]byte{0x00}),
			fdbx.LT([]byte{0xFF}),
		)
		return
	}))
	assert.Len(t, list, 2)
	assert.Equal(t, skey, string(list[0].ID()))
	assert.Equal(t, skey3, string(list[1].ID()))
	assert.Equal(t, string(tdata), string(list[0].Dump()))
	assert.Equal(t, string(tdata3), string(list[1].Dump()))

	// ************ DB.Drop ************

	assert.NoError(t, c1.Tx(func(db fdbx.DB) (e error) { return db.Drop(m1, m2, m3, m4) }))

	// assert.False(t, true)
}

func BenchmarkSaveOneBig(b *testing.B) {
	b.StopTimer()

	const db = 1
	const ctype = 2

	// overvalue for disable gzipping
	fdbx.GZipSize = 10000000
	fdbx.ChunkSize = fdbx.MaxChunkSize

	c, err := fdbx.NewConn(db, fdbx.ConnVersion610)
	assert.NoError(b, err)
	assert.NotNil(b, c)

	// 9 Mb no gzipped records
	uid := uuid.New()
	m := &testModel{key: uid.String(), ctype: ctype, data: bytes.Repeat(uid[:9], 1024*1024)}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		m.key = uuid.New().String()
		assert.NoError(b, c.Tx(func(db fdbx.DB) (e error) { return db.Save(m) }))
	}
}

func BenchmarkSaveMultiSmalls(b *testing.B) {
	b.StopTimer()

	const db = 1
	const ctype = 2

	// overvalue for disable gzipping
	fdbx.GZipSize = 10000000
	fdbx.ChunkSize = fdbx.MaxChunkSize

	c, err := fdbx.NewConn(db, fdbx.ConnVersion610)
	assert.NoError(b, err)
	assert.NotNil(b, c)

	// 8K with 1Kb no gzipped models
	count := 8000
	models := make([]fdbx.Model, count)

	for i := 0; i < count; i++ {
		uid := uuid.New()
		models[i] = &testModel{key: uid.String(), ctype: ctype, data: bytes.Repeat(uid[:8], 128)}
	}

	b.StartTimer()

	for i := 0; i < b.N; i++ {
		assert.NoError(b, c.Tx(func(db fdbx.DB) (e error) { return db.Save(models...) }))
	}
}
