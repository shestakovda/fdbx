package orm

import (
	"context"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/typex"
)

func NewQuery(tb Table, tx mvcc.Tx) Query {
	return &v1Query{
		tx: tx,
		tb: tb,
	}
}

func loadQuery(tb Table, tx mvcc.Tx, id string) (_ Query, err error) {
	var uid typex.UUID
	var pair fdbx.Pair

	if uid, err = typex.ParseUUID(id); err != nil {
		return nil, ErrLoadQuery.WithReason(err)
	}

	q := v1Query{
		tx: tx,
		tb: tb,

		queryid: uid,
	}

	if pair, err = tx.Select(WrapQueryKey(tb.ID(), fdbx.Bytes2Key(uid))); err != nil {
		return nil, ErrLoadQuery.WithReason(err)
	}

	val := pair.Value()

	if len(val) == 0 {
		return nil, ErrLoadQuery.WithReason(err)
	}

	cur := models.GetRootAsCursor(val, 0).UnPack()

	q.reverse = cur.Reverse
	q.idxtype = cur.IdxType
	q.iprefix = cur.IPrefix
	q.lastkey = cur.LastKey

	if q.idxtype > 0 {
		q.selector = NewIndexSelector(tx, q.idxtype, fdbx.Bytes2Key(q.iprefix))
	} else {
		q.selector = NewFullSelector(tx)
	}

	return &q, nil
}

type v1Query struct {
	tx mvcc.Tx
	tb Table

	// Сохраняемые значения (курсор)
	reverse bool
	idxtype uint16
	iprefix []byte
	lastkey []byte
	queryid typex.UUID

	// Текущие значения
	limit    int
	filters  []Filter
	selector Selector
}

func (q *v1Query) Reverse() Query {
	q.reverse = true
	return q
}

func (q *v1Query) Limit(lim int) Query {
	if lim > 0 {
		q.limit = lim
	}
	return q
}

func (q *v1Query) All() ([]fdbx.Pair, error) {
	list := make([]fdbx.Pair, 0, 128)
	pairs, errs := q.Sequence(context.Background())

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

	pairs, errs := q.Sequence(ctx)

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

	pairs, errs := q.Sequence(ctx)

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

	keys := make([]fdbx.Key, len(list))

	for i := range list {
		keys[i] = list[i].Key()
	}

	if err = q.tb.Delete(q.tx, keys...); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (q *v1Query) BySelector(sel Selector) Query {
	q.selector = sel
	return q
}

func (q *v1Query) ByID(ids ...fdbx.Key) Query {
	return q.BySelector(NewIDsSelector(q.tx, ids, true))
}

func (q *v1Query) PossibleByID(ids ...fdbx.Key) Query {
	return q.BySelector(NewIDsSelector(q.tx, ids, false))
}

func (q *v1Query) ByIndex(idx uint16, prefix fdbx.Key) Query {
	q.idxtype = idx
	q.iprefix = prefix.Bytes()
	return q.BySelector(NewIndexSelector(q.tx, idx, prefix))
}

func (q *v1Query) Where(hdl Filter) Query {
	if hdl != nil {
		q.filters = append(q.filters, hdl)
	}
	return q
}

func (q *v1Query) Sequence(ctx context.Context) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	if q.selector == nil {
		q.selector = NewFullSelector(q.tx)
	}

	go func() {
		var err error
		var need bool
		var pair fdbx.Pair

		defer close(list)
		defer close(errs)

		size := 0
		wctx, exit := context.WithCancel(ctx)
		pairs, errc := q.selector.Select(wctx, q.tb, LastKey(fdbx.Bytes2Key(q.lastkey)), Reverse(q.reverse))
		defer exit()

		for orig := range pairs {
			if pair, err = newUsrPair(q.tx, q.tb.ID(), orig); err != nil {
				errs <- ErrSequence.WithReason(err)
				return
			}

			if need, err = q.applyFilters(pair); err != nil {
				errs <- ErrSequence.WithReason(err)
				return
			}

			if !need {
				continue
			}

			select {
			case list <- pair:
				size++
			case <-ctx.Done():
				errs <- ErrSequence.WithReason(ctx.Err())
				return
			}

			if q.limit > 0 && size >= q.limit {
				return
			}
		}

		for err := range errc {
			if err != nil {
				errs <- ErrSequence.WithReason(err)
				return
			}
		}
	}()

	return list, errs
}

func (q *v1Query) Save() (cid string, err error) {
	if q.selector == nil {
		q.selector = NewFullSelector(q.tx)
	}

	q.lastkey = q.selector.LastKey().Bytes()

	if q.queryid == nil {
		q.queryid = typex.NewUUID()
	}

	cur := &models.CursorT{
		Reverse: q.reverse,
		IdxType: q.idxtype,
		LastKey: q.lastkey,
		IPrefix: q.iprefix,
		QueryID: q.queryid,
	}

	pairs := []fdbx.Pair{
		fdbx.NewPair(WrapQueryKey(q.tb.ID(), fdbx.Bytes2Key(q.queryid)), fdbx.FlatPack(cur)),
	}

	if err = q.tx.Upsert(pairs); err != nil {
		return "", ErrSaveQuery.WithReason(err)
	}

	return q.queryid.Hex(), nil
}

func (q *v1Query) Drop() (err error) {
	if q.queryid == nil {
		return nil
	}

	keys := []fdbx.Key{
		WrapQueryKey(q.tb.ID(), fdbx.Bytes2Key(q.queryid)),
	}

	if err = q.tx.Delete(keys); err != nil {
		return ErrDropQuery.WithReason(err)
	}

	return nil
}

func (q *v1Query) applyFilters(pair fdbx.Pair) (need bool, err error) {
	for i := range q.filters {
		if need, err = q.filters[i](pair); err != nil {
			return false, ErrFilter.WithReason(err)
		}

		if !need {
			return false, nil
		}
	}

	return true, nil
}
