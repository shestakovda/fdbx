package orm

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/errx"

	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewIDsSelector(tx mvcc.Tx, ids []fdb.Key, strict bool) Selector {
	return &idsSelector{
		tx:     tx,
		ids:    ids,
		strict: strict,
	}
}

type idsSelector struct {
	tx     mvcc.Tx
	ids    []fdb.Key
	strict bool
}

func (s *idsSelector) Select(ctx context.Context, tbl Table, args ...Option) (<-chan Selected, <-chan error) {
	list := make(chan Selected)
	errs := make(chan error, 1)

	go func() {
		var ok bool
		var err error
		var pair fdb.KeyValue
		var res map[string]fdb.KeyValue

		defer close(list)
		defer close(errs)

		opts := getOpts(args)
		rids := s.reversed(s.ids, opts.reverse)

		// Подготовка айдишек к выборке
		keys := make([]fdb.Key, len(rids))
		for i := range rids {
			keys[i] = WrapTableKey(tbl.ID(), rids[i])
		}

		// Запрашиваем сразу все
		if res, err = s.tx.SelectMany(keys); err != nil {
			errs <- ErrSelect.WithReason(err)
			return
		}

		// Выбираем результаты
		for i := range keys {
			if pair, ok = res[keys[i].String()]; !ok {
				if !s.strict {
					continue
				}

				errs <- ErrSelect.WithReason(ErrNotFound.WithReason(err).WithDebug(errx.Debug{
					"id": rids[i],
				}))
				return
			}

			select {
			case list <- Selected{rids[i], pair}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return list, errs
}

func (s idsSelector) reversed(a []fdb.Key, rev bool) []fdb.Key {
	if rev {
		for left, right := 0, len(a)-1; left < right; left, right = left+1, right-1 {
			a[left], a[right] = a[right], a[left]
		}
	}
	return a
}
