package orm

import (
	"context"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewIndexSelector(tx mvcc.Tx, idx uint16, prefix fdbx.Key) Selector {
	return NewIndexRangeSelector(tx, idx, prefix, prefix)
}

func NewIndexRangeSelector(tx mvcc.Tx, idx uint16, from, last fdbx.Key) Selector {
	return &indexSelector{
		tx:   tx,
		idx:  idx,
		from: from,
		last: last,
	}
}

type indexSelector struct {
	tx   mvcc.Tx
	idx  uint16
	from fdbx.Key
	last fdbx.Key
}

func (s *indexSelector) Select(ctx context.Context, tbl Table, args ...Option) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	go func() {
		var err error
		var pair fdbx.Pair

		defer close(list)
		defer close(errs)

		skip := false
		opts := getOpts(args)
		fkey := WrapIndexKey(tbl.ID(), s.idx, s.from)
		lkey := WrapIndexKey(tbl.ID(), s.idx, s.last)
		reqs := make([]mvcc.Option, 0, 3)

		if len(opts.lastkey.Bytes()) > 0 {
			skip = true
			lkey = WrapIndexKey(tbl.ID(), s.idx, opts.lastkey)
		}

		if opts.reverse {
			reqs = append(reqs, mvcc.From(fkey), mvcc.To(lkey), mvcc.Reverse())
		} else {
			reqs = append(reqs, mvcc.From(lkey), mvcc.To(fkey))
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

			if pair, err = s.tx.Select(WrapTableKey(tbl.ID(), fdbx.Bytes2Key(item.Value()))); err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}

			select {
			case list <- fdbx.WrapPair(UnwrapIndexKey(item.Key()), pair):
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
