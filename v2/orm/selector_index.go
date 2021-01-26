package orm

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/errx"

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

		wctx, exit := context.WithCancel(ctx)
		pairs, errc := s.tx.SeqScan(wctx, reqs...)
		defer exit()

		bufSize := 100
		buf := make([]fdb.KeyValue, 0, bufSize)

		for item := range pairs {
			// В случае реверса из середины интервала нужно пропускать первое значение (b)
			// Потому что драйвер выбирает отрезок (a, b] и у нас нет возможности уменьшить b
			if skip {
				skip = false
				continue
			}

			buf = append(buf, item)

			if len(buf) >= bufSize {
				if err = s.flush(ctx, tbl.ID(), buf, list); err != nil {
					errs <- ErrSelect.WithReason(err)
					return
				}
			}
		}

		if len(buf) > 0 {
			if err = s.flush(ctx, tbl.ID(), buf, list); err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}
		}

		for err = range errc {
			if err != nil {
				errs <- ErrSelect.WithReason(err)
				return
			}
		}
	}()

	return list, errs
}

func (s *indexSelector) flush(ctx context.Context, tid uint16, buf []fdb.KeyValue, list chan Selected) (err error) {
	var ok bool
	var pair fdb.KeyValue
	var res map[string]fdb.KeyValue

	rids := make([]fdb.Key, len(buf))
	for i := range buf {
		rids[i] = WrapTableKey(tid, buf[i].Value)
	}

	// Запрашиваем сразу все
	if res, err = s.tx.SelectMany(rids); err != nil {
		return
	}

	// Выбираем результаты
	for i := range rids {
		if pair, ok = res[rids[i].String()]; !ok {
			return ErrNotFound.WithReason(err).WithDebug(errx.Debug{
				"id": rids[i],
			})
		}

		select {
		case list <- Selected{UnwrapIndexKey(buf[i].Key), pair}:
		case <-ctx.Done():
			return
		}
	}

	return nil
}
