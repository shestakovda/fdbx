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
	w.tx.Clear(w.usrWrap(key).Raw())
}

func (w v610Writer) Upsert(pairs ...fdbx.Pair) {
	for i := range pairs {
		w.tx.Set(w.usrWrap(pairs[i].Key()).Raw(), pairs[i].Value())
	}
}

func (w v610Writer) Version() fdb.FutureKey {
	return w.tx.GetVersionstamp()
}

func (w v610Writer) Increment(key fdbx.Key, delta int64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], uint64(delta))
	w.tx.Add(w.usrWrap(key).Raw(), data[:])
}

func (w v610Writer) Erase(from, to fdbx.Key) {
	w.tx.ClearRange(fdb.KeyRange{
		Begin: w.usrWrap(from).Raw(),
		End:   w.endWrap(to).Raw(),
	})
}

func (w v610Writer) Watch(key fdbx.Key) fdbx.Waiter {
	return &v610Waiter{
		FutureNil: w.tx.Watch(w.usrWrap(key).Raw()),
	}
}

func (w v610Writer) Lock(from, to fdbx.Key) {
	w.tx.AddWriteConflictRange(fdb.KeyRange{
		Begin: w.usrWrap(from).Raw(),
		End:   w.endWrap(to).Raw(),
	})
}
