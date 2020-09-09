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

func (w v610Writer) Delete(key fdbx.Key) { w.tx.Clear(w.usrWrapper(key).Bytes()) }
func (w v610Writer) Upsert(pair fdbx.Pair) {
	w.tx.Set(pair.WrapKey(w.usrWrapper).Key().Bytes(), pair.Value())
}

func (w v610Writer) Versioned(key fdbx.Key) {
	var data [14]byte
	w.tx.SetVersionstampedValue(w.usrWrapper(key).Bytes(), data[:])
}

func (w v610Writer) Increment(key fdbx.Key, delta uint64) {
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], delta)
	w.tx.Add(w.usrWrapper(key).Bytes(), data[:])
}
