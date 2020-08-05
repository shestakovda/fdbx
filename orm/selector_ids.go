package orm

import "github.com/shestakovda/fdbx/mvcc"

func NewIDsSelector(tx mvcc.Tx, ids ...string) Selector {
	return &idsSelector{
		tx:  tx,
		ids: ids,
	}
}

type idsSelector struct {
	tx  mvcc.Tx
	ids []string
}

func (s *idsSelector) Select() (<-chan Model, <-chan error) {
	list := make(chan Model)
	errs := make(chan error, 1)

	go func() {
		var err error
		var value mvcc.Value

		defer close(list)
		defer close(errs)

		// TODO: параллельная или массовая загрузка
		for i := range s.ids {
			if value, err = s.tx.Select(mvcc.NewStrKey(s.ids[i])); err != nil {
				errs <- err
				return
			}

			mod := NewTableModel(s.ids[i])

			if err = mod.Unpack(value); err != nil {
				errs <- err
				return
			}

			list <- mod
		}
	}()

	return list, errs
}
