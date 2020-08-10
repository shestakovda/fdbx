package orm

import (
	"context"
	"runtime"

	"github.com/shestakovda/fdbx/mvcc"
)

func NewQuery(cl Collection, tx mvcc.Tx) Query {
	return &query{
		tx: tx,
		cl: cl,

		filters: make([]Filter, 0, 8),
	}
}

type query struct {
	tx mvcc.Tx
	cl Collection

	search  Selector
	filters []Filter

	rows chan Row
	errs chan error
}

func (q *query) ByID(ids ...mvcc.Key) Query {
	if q.search == nil {
		q.search = NewIDsSelector(q.tx, ids...)
	} else {
		q.filters = append(q.filters, NewIDsFilter(ids...))
	}
	return q
}

func (q *query) First(ctx context.Context) (res Model, err error) {
	if q.rows == nil {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		q.makeRows(wctx)
	}

	for r := range q.rows {
		if res, err = r.Model(); err != nil {
			return nil, ErrSelectFirst.WithReason(err)
		}

		return res, nil
	}

	for err = range q.errs {
		return nil, ErrSelectFirst.WithReason(err)
	}

	return nil, nil
}

func (q *query) All(ctx context.Context) (res []Model, err error) {
	var mod Model

	if q.rows == nil {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		q.makeRows(wctx)
	}

	res = make([]Model, 0, 64)
	for r := range q.rows {
		if mod, err = r.Model(); err != nil {
			return nil, ErrSelectAll.WithReason(err)
		}

		res = append(res, mod)
	}

	for err = range q.errs {
		return nil, ErrSelectAll.WithReason(err)
	}

	return res, nil
}

func (q *query) Agg(ctx context.Context, funcs ...AggFunc) (err error) {
	if q.rows == nil {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		q.makeRows(wctx)
	}

	for row := range q.rows {
		for i := range funcs {
			if err = funcs[i](row); err != nil {
				return ErrSelectAgg.WithReason(err)
			}
		}
	}

	for err = range q.errs {
		return ErrSelectAgg.WithReason(err)
	}

	return nil
}

func (q *query) Delete(ctx context.Context) (err error) {
	if q.rows == nil {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		q.makeRows(wctx)
	}

	for r := range q.rows {
		if err = q.tx.Delete(q.cl.SysKey(r.Key())); err != nil {
			return ErrDelete.WithReason(err)
		}
	}

	for err = range q.errs {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (q *query) makeRows(ctx context.Context) {
	q.rows = make(chan Row)
	q.errs = make(chan error, 1)

	if q.search == nil {
		q.search = NewFullSelector(q.tx, runtime.NumCPU())
	}

	go func() {
		var err error
		var skip bool

		defer close(q.rows)
		defer close(q.errs)

		list, errs := q.search.Select(ctx, q.cl)

	loop:
		for r := range list {
			for i := range q.filters {
				if skip, err = q.filters[i].Skip(r); err != nil {
					q.errs <- ErrStream.WithReason(err)
					return
				}

				if skip {
					continue loop
				}
			}

			select {
			case q.rows <- r:
			case <-ctx.Done():
				q.errs <- ErrStream.WithReason(ctx.Err())
				return
			}
		}

		for err = range errs {
			q.errs <- ErrStream.WithReason(err)
			return
		}
	}()
}
