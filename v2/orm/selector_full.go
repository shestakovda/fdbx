package orm

import (
	"context"

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

func (s fullSelector) Select(ctx context.Context, tbl Table) (<-chan fdbx.Pair, <-chan error) {
	key := tbl.Mgr().Wrap(nil)
	return s.tx.SeqScan(ctx, key, key)
}
