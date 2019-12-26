package fdbx

import "github.com/apple/foundationdb/bindings/go/src/fdb"

func newV610Indexer() (*v610Indexer, error) {
	return &v610Indexer{make([]*v610Index, 0, 16)}, nil
}

type v610Indexer struct {
	list []*v610Index
}

func (idx *v610Indexer) Index(idxTypeID uint16, value []byte) {
	idx.list = append(idx.list, &v610Index{
		typeID: idxTypeID,
		value:  value,
	})
}

func (idx *v610Indexer) commit(dbID uint16, tx fdb.Transaction, drop bool, rid, rln []byte) error {
	for i := range idx.list {
		if len(idx.list[i].value) == 0 {
			continue
		}

		key := fdbKey(dbID, idx.list[i].typeID, idx.list[i].value, rid, rln)

		if drop {
			tx.Clear(key)
		} else {
			tx.Set(key, nil)
		}
	}
	return nil
}

func (idx *v610Indexer) clear(dbID uint16, tx fdb.Transaction) (err error) {
	for i := range idx.list {
		if err = clearType(dbID, idx.list[i].typeID, tx); err != nil {
			return
		}
	}
	return nil
}

type v610Index struct {
	typeID uint16
	value  []byte
}
