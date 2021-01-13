package orm

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/golang/glog"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewIndexSelector(tx mvcc.Tx, idx uint16, prefix fdb.Key) Selector {
	return NewIndexRangeSelector(tx, idx, prefix, prefix)
}

func NewIndexRangeSelector(tx mvcc.Tx, idx uint16, from, last fdb.Key) Selector {
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
	from fdb.Key
	last fdb.Key
}

func (s *indexSelector) Select(ctx context.Context, tbl Table, args ...Option) (<-chan Selected, <-chan error) {
	list := make(chan Selected)
	errs := make(chan error, 1)

	go func() {
		var err error
		var pair fdb.KeyValue

		defer close(list)
		defer close(errs)

		fkey := s.from
		lkey := s.last
		opts := getOpts(args)
		skip := len(opts.lastkey) > 0

		if skip {
			if opts.reverse {
				lkey = opts.lastkey
			} else {
				fkey = opts.lastkey
			}
		}

		reqs := []mvcc.Option{
			mvcc.From(WrapIndexKey(tbl.ID(), s.idx, fkey)),
			mvcc.Last(WrapIndexKey(tbl.ID(), s.idx, lkey)),
		}

		if opts.reverse {
			reqs = append(reqs, mvcc.Reverse())
		}

		if Debug {
			glog.Infof("indexSelector.Select.fkey = %s", WrapIndexKey(tbl.ID(), s.idx, fkey))
			glog.Infof("indexSelector.Select.lkey = %s", WrapIndexKey(tbl.ID(), s.idx, lkey))
			glog.Infof("indexSelector.Select.skip = %t", skip)
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

			if pair, err = s.tx.Select(WrapTableKey(tbl.ID(), item.Value)); err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}

			select {
			case list <- Selected{UnwrapIndexKey(item.Key), pair}:
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
