package orm

import (
	"context"

	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewFullSelector(tx mvcc.Tx) Selector {
	return &fullSelector{
		tx: tx,
	}
}

type fullSelector struct {
	tx mvcc.Tx
}

func (s *fullSelector) Select(ctx context.Context, tbl Table, args ...Option) (<-chan Selected, <-chan error) {
	list := make(chan Selected)
	errs := make(chan error, 1)

	go func() {
		var reqs []mvcc.Option

		defer close(list)
		defer close(errs)

		opts := getOpts(args)
		fkey := WrapTableKey(tbl.ID(), opts.lastkey)
		lkey := WrapTableKey(tbl.ID(), nil)
		skip := len(opts.lastkey) > 0

		if opts.reverse {
			reqs = []mvcc.Option{mvcc.From(lkey), mvcc.Last(fkey), mvcc.Reverse()}
		} else {
			reqs = []mvcc.Option{mvcc.From(fkey), mvcc.Last(lkey)}
		}

		wctx, exit := context.WithCancel(ctx)
		pairs, errc := s.tx.SeqScan(wctx, reqs...)
		defer exit()

		for pair := range pairs {
			// В случае реверса из середины интервала нужно пропускать первое значение (b)
			// Потому что драйвер выбирает отрезок (a, b] и у нас нет возможности уменьшить b
			if skip {
				skip = false
				continue
			}

			select {
			case list <- Selected{UnwrapTableKey(pair.Key), pair}:
			case <-wctx.Done():
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
