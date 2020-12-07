package orm

import (
	"context"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewIDsSelector(tx mvcc.Tx, ids []fdbx.Key, strict bool) Selector {
	return &idsSelector{
		tx:     tx,
		ids:    ids,
		strict: strict,
	}
}

type idsSelector struct {
	tx     mvcc.Tx
	ids    []fdbx.Key
	strict bool
}

func (s *idsSelector) Select(ctx context.Context, tbl Table, args ...Option) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	go func() {
		var err error
		var pair fdbx.Pair

		defer close(list)
		defer close(errs)

		opts := getOpts(args)
		rids := s.reversed(s.ids, opts.reverse)

		for i := range rids {
			if pair, err = s.tx.Select(WrapTableKey(tbl.ID(), rids[i])); err != nil {
				if errx.Is(err, mvcc.ErrNotFound) {
					if !s.strict {
						continue
					}

					err = ErrNotFound.WithReason(err).WithDebug(errx.Debug{
						"id": rids[i].String(),
					})
				}

				errs <- ErrSelect.WithReason(err)
				return
			}

			select {
			case list <- fdbx.WrapPair(rids[i], pair):
			case <-ctx.Done():
				return
			}
		}
	}()

	return list, errs
}

func (s idsSelector) reversed(a []fdbx.Key, rev bool) []fdbx.Key {
	if rev {
		for left, right := 0, len(a)-1; left < right; left, right = left+1, right-1 {
			a[left], a[right] = a[right], a[left]
		}
	}
	return a
}
