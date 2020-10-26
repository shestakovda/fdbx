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

func (s fullSelector) Select(tbl Table) (list []fdbx.Pair, err error) {
	mgr := tbl.Mgr()
	key := mgr.Wrap(nil)

	if list, err = s.tx.SeqScan(key, key); err != nil {
		return nil, ErrSelect.WithReason(err)
	}

	valWrapper := sysValWrapper(s.tx, tbl.ID())

	for i := range list {
		list[i] = list[i].WrapKey(mgr.Unwrapper).WrapValue(valWrapper)
	}

	return list, nil
}
