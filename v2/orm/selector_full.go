package orm

import (
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
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
	var key fdbx.Key

	if key, err = usrKeyWrapper(cl.ID())(nil); err != nil {
		return nil, ErrSelect.WithReason(err)
	}

	if list, err = s.tx.SeqScan(key, key); err != nil {
		return nil, ErrSelect.WithReason(err)
	}

	valWrapper := sysValWrapper(s.tx, cl.ID())

	for i := range list {
		list[i] = list[i].WrapKey(sysKeyWrapper).WrapValue(valWrapper)
	}

	return list, nil
}
