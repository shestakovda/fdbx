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

func (s fullSelector) Select(ctx context.Context, tbl Table, args ...mvcc.Option) (<-chan fdbx.Pair, <-chan error) {
	return s.tx.SeqScan(ctx, append(args, mvcc.From(tbl.Mgr().Wrap(nil)))...)
}
