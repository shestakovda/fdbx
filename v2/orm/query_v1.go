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

	limit   int
	search  Selector
	filters []Filter
}

func (q *v1Query) Limit(lim int) Query {
	if lim > 0 {
		q.limit = lim
	}
	return q
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

func (q *v1Query) Agg(funcs ...Aggregator) (err error) {
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

func (q *v1Query) Where(hdl Filter) Query {
	if hdl != nil {
		q.filters = append(q.filters, hdl)
	}
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
		var need bool

		defer close(list)
		defer close(errs)

		size := 0
		kwrp := q.tb.Mgr().Unwrapper
		vwrp := sysValWrapper(q.tx, q.tb.ID())
		wctx, exit := context.WithCancel(ctx)
		pairs, errc := q.search.Select(wctx, q.tb)
		defer exit()

		for pair := range pairs {
			pair = pair.WrapKey(kwrp).WrapValue(vwrp)

			if need, err = q.applyFilters(pair); err != nil {
				errs <- err
				return
			}

			if !need {
				continue
			}

			select {
			case list <- pair:
				size++
			case <-ctx.Done():
				errs <- ErrSelect.WithReason(ctx.Err())
				return
			}

			if q.limit > 0 && size >= q.limit {
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

func (q *v1Query) applyFilters(pair fdbx.Pair) (need bool, err error) {
	for i := range q.filters {
		if need, err = q.filters[i](pair); err != nil {
			return false, ErrSelect.WithReason(err)
		}

		if !need {
			return false, nil
		}
	}

	return true, nil
}
