package orm

import (
	"context"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewQuery(tb Table, tx mvcc.Tx) Query {
	q := v1Query{
		tx: tx,
		tb: tb,

		filters: make([]Filter, 0, 8),
	}
	return &q
}

type v1Query struct {
	tx mvcc.Tx
	tb Table

	search  Selector
	filters []Filter
}

func (q *v1Query) All() ([]fdbx.Pair, error) {
	list := make([]fdbx.Pair, 0, 128)
	pairs, errs := q.filtered(context.Background())

	for pair := range pairs {
		list = append(list, pair)
	}

	for err := range errs {
		if err != nil {
			return nil, ErrAll.WithReason(err)
		}
	}

	return list, nil
}

func (q *v1Query) First() (fdbx.Pair, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pairs, errs := q.filtered(ctx)

	for pair := range pairs {
		return pair, nil
	}

	for err := range errs {
		if err != nil {
			return nil, ErrFirst.WithReason(err)
		}
	}

	return nil, nil
}

func (q *v1Query) Agg(funcs ...AggFunc) (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pairs, errs := q.filtered(ctx)

	for pair := range pairs {
		for i := range funcs {
			if err = funcs[i](pair); err != nil {
				return ErrAgg.WithReason(err)
			}
		}
	}

	for err = range errs {
		if err != nil {
			return ErrAgg.WithReason(err)
		}
	}

	return nil
}

func (q *v1Query) Delete() (err error) {
	var list []fdbx.Pair

	if list, err = q.All(); err != nil {
		return ErrDelete.WithReason(err)
	}

	if err = q.tb.Delete(q.tx, list...); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (q *v1Query) ByID(ids ...fdbx.Key) Query {
	q.search = NewIDsSelector(q.tx, ids, true)
	return q
}

func (q *v1Query) PossibleByID(ids ...fdbx.Key) Query {
	q.search = NewIDsSelector(q.tx, ids, false)
	return q
}

func (q *v1Query) ByIndex(idx uint16, prefix fdbx.Key) Query {
	q.search = NewIndexSelector(q.tx, idx, prefix)
	return q
}

func (q *v1Query) filtered(ctx context.Context) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	if q.search == nil {
		q.search = NewFullSelector(q.tx)
	}

	go func() {
		var err error
		var skip bool

		defer close(list)
		defer close(errs)

		kwrp := q.tb.Mgr().Unwrapper
		vwrp := sysValWrapper(q.tx, q.tb.ID())
		pairs, errc := q.search.Select(ctx, q.tb)

	Recs:
		for pair := range pairs {

			for j := range q.filters {
				if skip, err = q.filters[j].Skip(pair); err != nil {
					errs <- ErrSelect.WithReason(err)
					return
				}

				if skip {
					continue Recs
				}
			}

			select {
			case list <- pair.WrapKey(kwrp).WrapValue(vwrp):
			case <-ctx.Done():
				errs <- ErrSelect.WithReason(ctx.Err())
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
