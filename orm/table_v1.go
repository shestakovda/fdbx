package orm

import (
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx"
	"github.com/shestakovda/fdbx/mvcc"
)

func Table(id uint16, opts ...Option) Collection {
	t := v1Table{
		id:      id,
		options: newOptions(),
	}

	for i := range opts {
		opts[i](&t.options)
	}

	return &t
}

type v1Table struct {
	options
	id uint16
}

func (t v1Table) ID() uint16 { return t.id }

func (t v1Table) Select(tx mvcc.Tx) Query { return NewQuery(&t, tx) }

func (t v1Table) Upsert(tx mvcc.Tx, pairs ...fdbx.Pair) (err error) {
	if len(pairs) == 0 {
		return nil
	}

	cp := make([]fdbx.Pair, len(pairs))
	for i := range pairs {
		cp[i] = pairs[i].WrapKey(usrKeyWrapper(t.id)).WrapValue(usrValWrapper)
	}

	if err = tx.Upsert(cp, mvcc.OnDelete(t.onDelete)); err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

func (t v1Table) Delete(tx mvcc.Tx, pairs ...fdbx.Pair) (err error) {
	if len(pairs) == 0 {
		return nil
	}

	cp := make([]fdbx.Key, len(pairs))
	for i := range pairs {
		cp[i] = pairs[i].WrapKey(usrKeyWrapper(t.id)).Key()
	}

	if err = tx.Delete(cp, mvcc.OnDelete(t.onDelete)); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (t v1Table) onDelete(tx mvcc.Tx, pair fdbx.Pair) (err error) {
	if len(t.options.indexes) == 0 {
		return nil
	}

	var ups [1]fdbx.Key
	val := pair.Value()

	for idxid, fnc := range t.options.indexes {
		ups[0] = idxKeyWrapper(idxid)(fnc(val))
		if err = tx.Delete(ups[:]); err != nil {
			return ErrIdxDelete.WithReason(err).WithDebug(errx.Debug{"idx": idxid})
		}
	}

	return nil
}

func (t v1Table) onInsert(tx mvcc.Tx, pair fdbx.Pair) (err error) {
	if len(t.options.indexes) == 0 {
		return nil
	}

	var ups [1]fdbx.Pair
	val := pair.Value()

	for idxid, fnc := range t.options.indexes {
		ups[0] = fdbx.NewPair(fnc(val), fdbx.Value(pair.Key().Bytes())).WrapKey(idxKeyWrapper(idxid))
		if err = tx.Upsert(ups[:]); err != nil {
			return ErrIdxUpsert.WithReason(err).WithDebug(errx.Debug{"idx": idxid})
		}
	}

	return nil

}
