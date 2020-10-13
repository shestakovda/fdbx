package orm

import (
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
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

	valWrapper := usrValWrapper(tx, t.id)
	keyWrapper := usrKeyWrapper(t.id)

	cp := make([]fdbx.Pair, len(pairs))
	for i := range pairs {
		cp[i] = pairs[i].WrapKey(keyWrapper).WrapValue(valWrapper)
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

	keyWrapper := usrKeyWrapper(t.id)

	cp := make([]fdbx.Key, len(pairs))
	for i := range pairs {
		if cp[i], err = pairs[i].WrapKey(keyWrapper).Key(); err != nil {
			return ErrDelete.WithReason(err)
		}
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

	var val fdbx.Value
	var ups [1]fdbx.Key

	if val, err = pair.Value(); err != nil {
		return ErrIdxDelete.WithReason(err)
	}

	for idxid, fnc := range t.options.indexes {
		if ups[0] = fnc(val); ups[0] == nil {
			continue
		}

		if ups[0], err = idxKeyWrapper(t.id, idxid)(ups[0]); err != nil {
			return ErrIdxDelete.WithReason(err)
		}

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

	var val fdbx.Value
	var ups [1]fdbx.Pair
	var key, tmp fdbx.Key

	if val, err = pair.Value(); err != nil {
		return ErrIdxUpsert.WithReason(err)
	}

	for idxid, fnc := range t.options.indexes {
		if tmp = fnc(val); tmp == nil {
			continue
		}

		if key, err = pair.Key(); err != nil {
			return ErrIdxUpsert.WithReason(err)
		}

		ups[0] = fdbx.NewPair(tmp, fdbx.Value(key.Bytes())).WrapKey(idxKeyWrapper(t.id, idxid))
		if err = tx.Upsert(ups[:]); err != nil {
			return ErrIdxUpsert.WithReason(err).WithDebug(errx.Debug{"idx": idxid})
		}
	}

	return nil

}
