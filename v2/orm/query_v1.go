package orm

import (
	"context"
	"sync/atomic"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/typex"
)

func NewQuery(tb Table, tx mvcc.Tx) Query {
	q := &v1Query{
		tx: tx,
		tb: tb,
	}

	q.lastkey.Store(fdbx.Bytes2Key(nil))
	return q
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
		return nil, ErrLoadQuery.WithReason(ErrNotFound.WithReason(err))
	}

	val := pair.Value()

	if len(val) == 0 {
		return nil, ErrLoadQuery.WithReason(ErrValUnpack.WithReason(err))
	}

	cur := models.GetRootAsCursor(val, 0).UnPack()

	q.size = cur.Size
	q.page = cur.Page
	q.limit = cur.Limit
	q.reverse = cur.Reverse
	q.idxtype = cur.IdxType
	q.idxfrom = cur.IdxFrom
	q.idxlast = cur.IdxLast
	q.lastkey.Store(fdbx.Bytes2Key(cur.LastKey))

	if q.idxtype > 0 {
		q.selector = NewIndexRangeSelector(tx, q.idxtype, fdbx.Bytes2Key(q.idxfrom), fdbx.Bytes2Key(q.idxlast))
	} else {
		q.selector = NewFullSelector(tx)
	}

	return &q, nil
}

type v1Query struct {
	tx mvcc.Tx
	tb Table

	// Сохраняемые значения (курсор)
	size    uint32
	page    uint32
	limit   uint32
	empty   bool
	reverse bool
	idxtype uint16
	idxfrom []byte
	idxlast []byte
	queryid typex.UUID
	lastkey atomic.Value

	// Текущие значения
	filters  []Filter
	selector Selector
}

func (q *v1Query) Empty() bool { return q.empty }

func (q *v1Query) Reverse() Query {
	q.reverse = true
	return q
}

func (q *v1Query) Limit(lim int) Query {
	if lim > 0 {
		q.limit = uint32(lim)
	}
	return q
}

func (q *v1Query) Page(size int) Query {
	if size > 0 {
		q.page = uint32(size)
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

func (q *v1Query) Next() (_ []fdbx.Pair, err error) {
	defer func() { _, err = q.Save() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	size := uint32(0)
	list := make([]fdbx.Pair, 0, q.page)
	pairs, errs := q.Sequence(ctx)

	for pair := range pairs {
		list = append(list, pair)
		if size++; q.page > 0 && size >= q.page {
			return list, nil
		}
	}

	for err := range errs {
		if err != nil {
			return nil, ErrNext.WithReason(err)
		}
	}

	q.empty = true
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

	return nil, ErrNotFound.WithStack()
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
	q.idxfrom = prefix.Bytes()
	q.idxlast = prefix.Bytes()
	return q.BySelector(NewIndexSelector(q.tx, idx, prefix))
}

func (q *v1Query) ByIndexRange(idx uint16, from, last fdbx.Key) Query {
	q.idxtype = idx
	q.idxfrom = from.Bytes()
	q.idxlast = last.Bytes()
	return q.BySelector(NewIndexRangeSelector(q.tx, idx, from, last))
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

	q.empty = false
	if q.selector == nil {
		q.selector = NewFullSelector(q.tx)
	}

	go func() {
		var err error
		var need bool
		var size uint32
		var pair fdbx.Pair

		defer close(list)
		defer close(errs)

		if size = atomic.LoadUint32(&q.size); q.limit > 0 && size >= q.limit {
			return
		}

		wctx, exit := context.WithCancel(ctx)
		pairs, errc := q.selector.Select(wctx, q.tb, LastKey(q.lastkey.Load().(fdbx.Key)), Reverse(q.reverse))
		defer exit()

		for orig := range pairs {
			if pair, err = newUsrPair(q.tx, q.tb.ID(), orig.Unwrap()); err != nil {
				errs <- ErrSequence.WithReason(err)
				return
			}

			if len(q.filters) > 0 {
				if need, err = q.applyFilters(pair); err != nil {
					errs <- ErrSequence.WithReason(err)
					return
				}

				if !need {
					continue
				}
			}

			select {
			case list <- pair:
				q.lastkey.Store(orig.Key())

				if size = atomic.AddUint32(&q.size, 1); q.limit > 0 && size >= q.limit {
					return
				}
			case <-wctx.Done():
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
	if q.queryid == nil {
		q.queryid = typex.NewUUID()
	}

	cur := &models.CursorT{
		Size:    atomic.LoadUint32(&q.size),
		Page:    q.page,
		Limit:   q.limit,
		Reverse: q.reverse,
		IdxType: q.idxtype,
		IdxFrom: q.idxfrom,
		IdxLast: q.idxlast,
		QueryID: q.queryid,
		LastKey: q.lastkey.Load().(fdbx.Key).Bytes(),
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
