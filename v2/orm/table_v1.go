package orm

import (
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewTable(id uint16, opts ...Option) Table {
	t := v1Table{
		id:      id,
		mgr:     newTableKeyManager(id),
		options: newOptions(),
	}

	for i := range opts {
		opts[i](&t.options)
	}

	t.idx = make(map[uint16]fdbx.KeyManager, len(t.options.indexes))
	for idx := range t.options.indexes {
		t.idx[idx] = newIndexKeyManager(id, idx)
	}

	return &t
}

type v1Table struct {
	options
	id  uint16
	mgr fdbx.KeyManager
	idx map[uint16]fdbx.KeyManager
}

func (t v1Table) ID() uint16 { return t.id }

func (t v1Table) Mgr() fdbx.KeyManager { return t.mgr }

func (t v1Table) Select(tx mvcc.Tx) Query { return NewQuery(&t, tx) }

func (t v1Table) Upsert(tx mvcc.Tx, pairs ...fdbx.Pair) (err error) {
	if len(pairs) == 0 {
		return nil
	}

	valWrapper := usrValWrapper(tx, t.id)

	cp := make([]fdbx.Pair, len(pairs))
	for i := range pairs {
		cp[i] = pairs[i].WrapKey(t.mgr.Wrapper).WrapValue(valWrapper)
	}

	if err = tx.Upsert(cp, mvcc.OnInsert(t.onInsert), mvcc.OnDelete(t.onDelete)); err != nil {
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
		if cp[i], err = pairs[i].WrapKey(t.mgr.Wrapper).Key(); err != nil {
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

	var val []byte
	var ups [1]fdbx.Key

	if val, err = pair.Value(); err != nil {
		return ErrIdxDelete.WithReason(err)
	}

	for idx, fnc := range t.options.indexes {
		if ups[0] = fnc(val); ups[0] == nil {
			continue
		}

		ups[0] = t.idx[idx].Wrap(ups[0])

		if err = tx.Delete(ups[:]); err != nil {
			return ErrIdxDelete.WithReason(err).WithDebug(errx.Debug{"idx": idx})
		}
	}

	return nil
}

func (t v1Table) onInsert(tx mvcc.Tx, pair fdbx.Pair) (err error) {
	if len(t.options.indexes) == 0 {
		return nil
	}

	var val []byte
	var ups [1]fdbx.Pair
	var key, tmp fdbx.Key

	if val, err = pair.Value(); err != nil {
		return ErrIdxUpsert.WithReason(err)
	}

	for idx, fnc := range t.options.indexes {
		if tmp = fnc(val); tmp == nil {
			continue
		}

		if key, err = pair.Key(); err != nil {
			return ErrIdxUpsert.WithReason(err)
		}

		ups[0] = fdbx.NewPair(tmp, t.idx[idx].Wrap(key))
		if err = tx.Upsert(ups[:]); err != nil {
			return ErrIdxUpsert.WithReason(err).WithDebug(errx.Debug{"idx": idx})
		}
	}

	return nil

}
