package fdbx

import "github.com/apple/foundationdb/bindings/go/src/fdb"

func newV610Indexer() (*v610Indexer, error) {
	return &v610Indexer{make([]*v610Index, 0, 16)}, nil
}

type v610Indexer struct {
	list []*v610Index
}

func (idx *v610Indexer) Index(idxTypeID uint16, value []byte) {
	if len(value) == 0 {
		return
	}

	idx.list = append(idx.list, &v610Index{
		typeID: idxTypeID,
		value:  value,
	})
}

func (idx *v610Indexer) commit(dbID uint16, tx fdb.Transaction, drop bool, rid, rln []byte) error {
	for i := range idx.list {
		key := fdbKey(dbID, idx.list[i].typeID, idx.list[i].value, rid, rln)

		if drop {
			tx.Clear(key)
		} else {
			tx.Set(key, nil)
		}
	}
	return nil
}

type v610Index struct {
	typeID uint16
	value  []byte
}
