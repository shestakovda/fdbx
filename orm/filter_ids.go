package orm

import "github.com/shestakovda/fdbx/mvcc"

func NewIDsFilter(ids ...mvcc.Key) Filter {
	return &idsFilter{
		ids: ids,
	}
}

type idsFilter struct {
	ids []mvcc.Key
}

func (f *idsFilter) Skip(m Model) (bool, error) {
	return false, nil
}