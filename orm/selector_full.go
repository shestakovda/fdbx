package orm

import (
	"github.com/shestakovda/fdbx"
	"github.com/shestakovda/fdbx/mvcc"
)

func NewFullSelector(tx mvcc.Tx) Selector {
	s := fullSelector{
		tx: tx,
	}
	return &s
}

type fullSelector struct {
	tx mvcc.Tx
}

func (s fullSelector) Select(cl Collection) (list []fdbx.Pair, err error) {
	key := usrKeyWrapper(cl.ID())(nil)

	if list, err = s.tx.SeqScan(key, key); err != nil {
		return nil, ErrSelect.WithReason(err)
	}

	for i := range list {
		list[i] = list[i].WrapKey(sysKeyWrapper).WrapValue(sysValWrapper)
	}

	return list, nil
}
