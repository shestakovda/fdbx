package orm

import (
	"context"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewIDsSelector(tx mvcc.Tx, ids []fdbx.Key, strict bool) Selector {
	s := idsSelector{
		tx:     tx,
		ids:    ids,
		strict: strict,
	}
	return &s
}

type idsSelector struct {
	tx     mvcc.Tx
	ids    []fdbx.Key
	strict bool
}

func (s idsSelector) Select(ctx context.Context, tbl Table, args ...mvcc.Option) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	go func() {
		var err error
		var pair fdbx.Pair

		defer close(list)
		defer close(errs)

		kwrp := tbl.Mgr().Wrap

		for i := range s.ids {
			if pair, err = s.tx.Select(kwrp(s.ids[i])); err != nil {
				if errx.Is(err, mvcc.ErrNotFound) {
					if !s.strict {
						continue
					}

					err = ErrNotFound.WithReason(err).WithDebug(errx.Debug{
						"id": s.ids[i].String(),
					})
				}

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
	}()

	return list, errs
}
