package orm

import (
	"context"

	"github.com/shestakovda/fdbx/mvcc"
)

func NewTableQuery(cl Collection, tx mvcc.Tx) Query {
	return &tableQuery{
		tx: tx,
		cl: cl,

		filters: make([]Filter, 8),
	}
}

type tableQuery struct {
	tx mvcc.Tx
	cl Collection

	search  Selector
	filters []Filter

	errs   chan error
	stream chan Model
}

func (q *tableQuery) ByID(ids ...string) Query {
	if q.search == nil {
		q.search = NewIDsSelector(q.tx, ids...)
	} else {
		q.filters = append(q.filters, NewIDsFilter(ids...))
	}
	return q
}

func (q *tableQuery) First() (Model, error) {

	if q.stream == nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		q.makeStream(ctx)
	}

	for m := range q.stream {
		return m, nil
	}

	for err := range q.errs {
		return nil, ErrSelect.WithReason(err)
	}

	return nil, nil
}

func (q *tableQuery) Delete() (err error) {
	if q.stream == nil {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		q.makeStream(ctx)
	}

	for m := range q.stream {
		if err = q.tx.Delete(m.ID()); err != nil {
			return ErrDelete.WithReason(err)
		}
	}

	for err = range q.errs {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (q *tableQuery) makeStream(ctx context.Context) {
	q.stream = make(chan Model)
	q.errs = make(chan error, 1)

	go func() {
		var err error
		var skip bool

		defer close(q.stream)
		defer close(q.errs)

		list, errs := q.search.Select()

	loop:
		for m := range list {
			for i := range q.filters {
				if skip, err = q.filters[i].Skip(m); err != nil {
					q.errs <- ErrFilter.WithReason(err)
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
