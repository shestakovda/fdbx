package db

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// WriteHandler - обработчик физической транзакции записи, должен быть идемпотентным
type WriteHandler func(Writer) error

type Writer struct {
	Reader
	tx fdb.Transaction
}

func (w Writer) Delete(key fdb.Key) {
	w.tx.Clear(w.usrWrap(key))
}

func (w Writer) Upsert(pairs ...fdb.KeyValue) {
	for i := range pairs {
		w.tx.Set(w.usrWrap(pairs[i].Key), pairs[i].Value)
	}
}

func (w Writer) Increment(key fdb.Key, delta int64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], uint64(delta))
	w.tx.Add(w.usrWrap(key), data[:])
}

func (w Writer) Erase(from, to fdb.Key) {
	w.tx.ClearRange(fdb.KeyRange{
		Begin: w.usrWrap(from),
		End:   w.endWrap(to),
	})
}

func (w Writer) Watch(key fdb.Key) Waiter {
	return &keyWaiter{
		FutureNil: w.tx.Watch(w.usrWrap(key)),
	}
}

func (w Writer) Lock(from, to fdb.Key) {
	w.tx.AddWriteConflictRange(fdb.KeyRange{
		Begin: w.usrWrap(from),
		End:   w.endWrap(to),
	})
}
