package orm

import (
	"context"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewIndexSelector(tx mvcc.Tx, idx uint16, prefix fdbx.Key) Selector {
	s := indexSelector{
		tx:     tx,
		idx:    idx,
		prefix: prefix,
	}
	return &s
}

type indexSelector struct {
	tx     mvcc.Tx
	idx    uint16
	prefix fdbx.Key
}

func (s indexSelector) Select(ctx context.Context, tbl Table, args ...mvcc.Option) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	go func() {
		var err error
		var val []byte
		var pair fdbx.Pair

		defer close(list)
		defer close(errs)

		kwrp := tbl.Mgr().Wrap
		ikey := newIndexKeyManager(tbl.ID(), s.idx).Wrap(nil)
		pairs, errc := s.tx.SeqScan(ctx, append(args, mvcc.From(ikey))...)

		for item := range pairs {
			if val, err = item.Value(); err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}

			if pair, err = s.tx.Select(kwrp(fdbx.Key(val))); err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}

			select {
			case list <- pair:
			case <-ctx.Done():
				errs <- ErrSelect.WithReason(ctx.Err())
				return
			}
		}

		for err := range errc {
			if err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}
		}
	}()

	return list, errs
}
