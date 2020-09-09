package orm

import (
	"github.com/shestakovda/fdbx"
	"github.com/shestakovda/fdbx/mvcc"
)

func NewQuery(cl Collection, tx mvcc.Tx) Query {
	q := v1Query{
		tx: tx,
		cl: cl,

		filters: make([]Filter, 0, 8),
	}
	return &q
}

type v1Query struct {
	tx mvcc.Tx
	cl Collection

	search  Selector
	filters []Filter
}

func (q *v1Query) All() ([]fdbx.Pair, error) { return q.filtered() }

func (q *v1Query) First() (_ fdbx.Pair, err error) {
	var list []fdbx.Pair

	if list, err = q.filtered(); err != nil {
		return
	}

	if len(list) == 0 {
		return nil, nil
	}

	return list[0], nil
}

func (q *v1Query) Agg(funcs ...AggFunc) (err error) {
	var list []fdbx.Pair

	if list, err = q.filtered(); err != nil {
		return
	}

	for i := range funcs {
		for j := range list {
			if err = funcs[i](list[j]); err != nil {
				return ErrAgg.WithReason(err)
			}
		}
	}

	return nil
}

func (q *v1Query) Delete() (err error) {
	var list []fdbx.Pair

	if list, err = q.filtered(); err != nil {
		return
	}

	if err = q.cl.Delete(q.tx, list...); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (q *v1Query) filtered() (res []fdbx.Pair, err error) {
	var skip bool
	var list []fdbx.Pair

	if q.search == nil {
		q.search = NewFullSelector(q.tx)
	}

	if list, err = q.search.Select(q.cl); err != nil {
		return
	}

	res = list[:0]

	for i := range list {
		need := true

		for j := range q.filters {
			if skip, err = q.filters[j].Skip(list[i]); err != nil {
				return nil, ErrSelect.WithReason(err)
			}

			if skip {
				need = false
				break
			}
		}

		if need {
			res = append(res, list[i])
		}
	}

	// Стираем хвост, чтобы сборщик мусора пришел за ним
	for i := len(res); i < len(list); i++ {
		list[i] = nil
	}

	return res, nil
}
