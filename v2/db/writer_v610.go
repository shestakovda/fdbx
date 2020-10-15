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

func (w v610Writer) Delete(key fdbx.Key) {
	w.tx.Clear(w.usrWrap(key).Bytes())
}

func (w v610Writer) Upsert(pair fdbx.Pair) (err error) {
	var key fdbx.Key
	var val fdbx.Value

	if key, err = pair.Key(); err != nil {
		return
	}

	if val, err = pair.Value(); err != nil {
		return
	}

	w.tx.Set(w.usrWrap(key).Bytes(), val)
	return nil
}

func (w v610Writer) Versioned(key fdbx.Key) {
	var data [14]byte
	w.tx.SetVersionstampedValue(w.usrWrap(key).Bytes(), data[:])
}

func (w v610Writer) Increment(key fdbx.Key, delta int64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], uint64(delta))
	w.tx.Add(w.usrWrap(key).Bytes(), data[:])
}

func (w v610Writer) Erase(from, to fdbx.Key) {
	w.tx.ClearRange(fdb.KeyRange{
		Begin: w.usrWrap(from).Bytes(),
		End:   w.endWrap(to).Bytes(),
	})
}

func (w v610Writer) Watch(key fdbx.Key) Waiter {
	return &v610Waiter{
		FutureNil: w.tx.Watch(w.usrWrap(key).Bytes()),
	}
}

func (w v610Writer) Lock(from, to fdbx.Key) {
	w.tx.AddWriteConflictRange(fdb.KeyRange{
		Begin: w.usrWrap(from).Bytes(),
		End:   w.endWrap(to).Bytes(),
	})
}
