package fdbx_test

import (
	"errors"
	"testing"

	"github.com/shestakovda/fdbx"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	const db = 1
	const skey = "test key"
	const skey2 = "test key 2"
	const ctype = 2

	var data []byte
	var tkey = []byte(skey)
	var tdata = []byte("test data")
	var tdata2 = []byte("test data 2")
	var terr = errors.New("test err")

	c1, err := fdbx.NewConn(db, fdbx.ConnVersion610)
	assert.NoError(t, err)
	assert.NotNil(t, c1)

	// ************ Key ************

	_, err = c1.Key(0, nil)
	assert.True(t, errors.Is(err, fdbx.ErrEmptyID))

	key, err := c1.Key(ctype, tkey)
	assert.NoError(t, err)
	assert.Equal(t, append([]byte{0, 1, 0, 2}, tkey...), key)

	// ************ MKey ************

	_, err = c1.MKey(nil)
	assert.True(t, errors.Is(err, fdbx.ErrNullModel))

	key, err = c1.MKey(&testModel{key: skey, ctype: ctype})
	assert.NoError(t, err)
	assert.Equal(t, append([]byte{0, 1, 0, 2}, tkey...), key)

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

	fdbx.GZipSize = 0
	fdbx.ChunkSize = 2

	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Save(m1, m2) })
	assert.NoError(t, err)

	m3 := &testModel{key: skey, ctype: ctype}
	m4 := &testModel{key: skey2, ctype: ctype}

	err = c1.Tx(func(db fdbx.DB) (e error) { return db.Load(m3, m4) })
	assert.NoError(t, err)

	assert.Equal(t, m1.Dump(), m3.Dump())
	assert.Equal(t, m2.Dump(), m4.Dump())
}
