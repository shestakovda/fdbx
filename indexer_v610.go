package fdbx

func newV610Indexer() (*v610Indexer, error) {
	return &v610Indexer{make([]*v610Index, 0, 16)}, nil
}

type v610Indexer struct {
	list []*v610Index
}

func (i *v610Indexer) Index(idxTypeID uint16, value []byte) {
	if len(value) == 0 {
		return
	}

	i.list = append(i.list, &v610Index{
		typeID: idxTypeID,
		value:  value,
	})
}

type v610Index struct {
	typeID uint16
	value  []byte
}
