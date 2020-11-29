package orm

import (
	"context"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewFullSelector(tx mvcc.Tx) Selector {
	return &fullSelector{
		baseSelector: newBaseSelector(tx),
	}
}

type fullSelector struct {
	*baseSelector
}

func (s *fullSelector) Select(ctx context.Context, tbl Table, args ...Option) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	go func() {
		var err error

		defer close(list)
		defer close(errs)

		opts := getOpts(args)
		nkey := WrapTableKey(tbl.ID(), nil)
		lkey := WrapTableKey(tbl.ID(), opts.lastkey)
		reqs := make([]mvcc.Option, 0, 3)
		skip := len(opts.lastkey.Bytes()) > 0

		if opts.reverse {
			reqs = append(reqs, mvcc.From(nkey), mvcc.To(lkey), mvcc.Reverse())
		} else {
			reqs = append(reqs, mvcc.From(lkey), mvcc.To(nkey))
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

			if err = s.sendPair(wctx, list, pair); err != nil {
				errs <- err
				return
			}

			s.setKey(UnwrapTableKey(pair.Key()))
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
