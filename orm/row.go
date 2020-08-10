package orm

import "github.com/shestakovda/fdbx/mvcc"

type row struct {
	key mvcc.Key
	val mvcc.Value
	mod Model
	fab ModelFabric
}

func (r *row) Key() mvcc.Key     { return r.key }
func (r *row) Value() mvcc.Value { return r.val }

func (r *row) Model() (_ Model, err error) {
	if r.mod == nil {
		r.mod = r.fab(r.key)
		if err = r.mod.Unpack(r.val); err != nil {
			return nil, ErrRowModel.WithReason(err)
		}
	}
	return r.mod, nil
}
