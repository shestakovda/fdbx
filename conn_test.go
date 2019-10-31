package fdbx_test

import (
	"errors"
	"testing"

	"github.com/shestakovda/fdbx"
	"github.com/stretchr/testify/assert"
)

func TestConn(t *testing.T) {
	const db = 1
	const ctype = 2
	var data []byte
	var tkey = []byte("test key")
	var tdata = []byte("test data")

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
}
