package db

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/fdbx/v2"
)

type v610Writer struct {
	v610Reader
	tx fdb.Transaction
}

func (w v610Writer) Delete(key fdbx.Key) (err error) {
	var wrk fdbx.Key

	if wrk, err = w.usrWrapper(key); err != nil {
		return
	}

	w.tx.Clear(wrk.Bytes())
	return nil
}

func (w v610Writer) Upsert(pair fdbx.Pair) (err error) {
	var key fdbx.Key
	var val fdbx.Value

	if key, err = pair.WrapKey(w.usrWrapper).Key(); err != nil {
		return
	}

	if val, err = pair.Value(); err != nil {
		return
	}

	w.tx.Set(key.Bytes(), val)
	return nil
}

func (w v610Writer) Versioned(key fdbx.Key) (err error) {
	var wrk fdbx.Key
	var data [14]byte

	if wrk, err = w.usrWrapper(key); err != nil {
		return
	}

	w.tx.SetVersionstampedValue(wrk.Bytes(), data[:])
	return nil
}

func (w v610Writer) Increment(key fdbx.Key, delta uint64) (err error) {
	var wrk fdbx.Key
	var data [8]byte

	if wrk, err = w.usrWrapper(key); err != nil {
		return
	}

	binary.LittleEndian.PutUint64(data[:], delta)
	w.tx.Add(wrk.Bytes(), data[:])
	return nil
}
