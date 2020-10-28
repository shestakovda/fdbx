package orm

import (
	"context"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewIndexSelector(tx mvcc.Tx, idx uint16, prefix fdbx.Key) Selector {
	s := indexSelector{
		idx:    idx,
		prefix: prefix,

		baseSelector: newBaseSelector(tx),
	}
	return &s
}

type indexSelector struct {
	*baseSelector

	idx    uint16
	prefix fdbx.Key
}

func (s *indexSelector) Select(ctx context.Context, tbl Table, args ...Option) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	go func() {
		var err error
		var val []byte
		var pair fdbx.Pair

		defer close(list)
		defer close(errs)

		opts := getOpts(args)
		kwrp := tbl.Mgr().Wrap
		imgr := newIndexKeyManager(tbl.ID(), s.idx)
		lkey := imgr.Wrap(opts.lastkey)
		reqs := make([]mvcc.Option, 0, 3)
		skip := opts.lastkey != nil

		if opts.reverse {
			reqs = append(reqs, mvcc.Reverse(), mvcc.From(imgr.Wrap(nil)), mvcc.To(lkey))
		} else {
			reqs = append(reqs, mvcc.From(lkey), mvcc.To(imgr.Wrap(nil)))
		}

		wctx, exit := context.WithCancel(ctx)
		pairs, errc := s.tx.SeqScan(wctx, reqs...)
		defer exit()

		for item := range pairs {
			// В случае реверса из середины интервала нужно пропускать первое значение (b)
			// Потому что драйвер выбирает отрезок (a, b] и у нас нет возможности уменьшить b
			if skip {
				skip = false
				continue
			}

			if val, err = item.Value(); err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}

			if pair, err = s.tx.Select(kwrp(fdbx.Key(val))); err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}

			if err = s.sendPair(wctx, list, pair); err != nil {
				errs <- err
				return
			}

			if s.lk, err = item.WrapKey(imgr.Unwrapper).Key(); err != nil {
				errs <- ErrSelect.WithReason(err)
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
