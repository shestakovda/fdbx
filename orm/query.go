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

	errs   chan error
	stream chan Model
}

func (q *query) ByID(ids ...mvcc.Key) Query {
	if q.search == nil {
		q.search = NewIDsSelector(q.tx, ids...)
	} else {
		q.filters = append(q.filters, NewIDsFilter(ids...))
	}
	return q
}

func (q *query) First(ctx context.Context) (Model, error) {
	if q.stream == nil {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		q.makeStream(wctx)
	}

	for m := range q.stream {
		return m, nil
	}

	for err := range q.errs {
		return nil, ErrSelectFirst.WithReason(err)
	}

	return nil, nil
}

func (q *query) All(ctx context.Context) ([]Model, error) {
	if q.stream == nil {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		q.makeStream(wctx)
	}

	res := make([]Model, 0, 64)
	for m := range q.stream {
		res = append(res, m)
	}

	for err := range q.errs {
		return nil, ErrSelectAll.WithReason(err)
	}

	return res, nil
}

func (q *query) Delete(ctx context.Context) (err error) {
	if q.stream == nil {
		wctx, cancel := context.WithCancel(ctx)
		defer cancel()
		q.makeStream(wctx)
	}

	for m := range q.stream {
		if err = q.tx.Delete(q.cl.SysKey(m.Key())); err != nil {
			return ErrDelete.WithReason(err)
		}
	}

	for err = range q.errs {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (q *query) makeStream(ctx context.Context) {
	q.stream = make(chan Model)
	q.errs = make(chan error, 1)

	if q.search == nil {
		q.search = NewFullSelector(q.tx, runtime.NumCPU())
	}

	go func() {
		var err error
		var skip bool

		defer close(q.stream)
		defer close(q.errs)

		list, errs := q.search.Select(ctx, q.cl)

	loop:
		for m := range list {
			for i := range q.filters {
				if skip, err = q.filters[i].Skip(m); err != nil {
					q.errs <- ErrStream.WithReason(err)
					return
				}

				if skip {
					continue loop
				}
			}

			select {
			case q.stream <- m:
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
