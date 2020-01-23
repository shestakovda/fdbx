package fdbx

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newV610Indexer() (*v610Indexer, error) {
	return new(v610Indexer), nil
}

type v610Indexer struct {
	types []uint16
	bytes [][]byte
}

func (idx *v610Indexer) Grow(n int) {
	if idx.types == nil {
		idx.types = make([]uint16, 0, n)
	}

	if idx.bytes == nil {
		idx.bytes = make([][]byte, 0, n)
	}

	// todo: grow existed
}

func (idx *v610Indexer) Index(idxTypeID uint16, value []byte) {
	idx.types = append(idx.types, idxTypeID)
	idx.bytes = append(idx.bytes, value)
}

func (idx *v610Indexer) commit(dbID uint16, tx fdb.Transaction, drop bool, id string) error {
	var key fdb.Key

	rid := S2B(id)
	buf := new(bytes.Buffer)
	rln := []byte{byte(len(rid))}

	for i := range idx.types {
		if len(idx.bytes[i]) == 0 {
			continue
		}

		buf.Reset()
		key = fdbKeyBuf(buf, dbID, idx.types[i], idx.bytes[i], rid, rln)

		if drop {
			tx.Clear(key)
		} else {
			tx.Set(key, nil)
		}
	}
	return nil
}

func (idx *v610Indexer) clear(dbID uint16, tx fdb.Transaction) (err error) {
	for i := range idx.types {
		if err = clearType(dbID, idx.types[i], tx); err != nil {
			return
		}
	}
	return nil
}
