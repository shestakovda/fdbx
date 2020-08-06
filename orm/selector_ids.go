package orm

import (
	"context"

	"github.com/shestakovda/fdbx/mvcc"
)

func NewIDsSelector(tx mvcc.Tx, ids ...mvcc.Key) Selector {
	return &idsSelector{
		tx:  tx,
		ids: ids,
	}
}

type idsSelector struct {
	tx  mvcc.Tx
	ids []mvcc.Key
}

func (s *idsSelector) Select(ctx context.Context, cl Collection) (<-chan Model, <-chan error) {
	list := make(chan Model)
	errs := make(chan error, 1)

	go func() {
		var err error
		var value mvcc.Value

		defer close(list)
		defer close(errs)

		// TODO: параллельная или массовая загрузка
		for i := range s.ids {
			if value, err = s.tx.Select(cl.ID2Key(s.ids[i])); err != nil {
				errs <- ErrSelectByID.WithReason(err)
				return
			}

			mod := cl.Fabric()(s.ids[i])

			if err = mod.Unpack(value); err != nil {
				errs <- ErrSelectByID.WithReason(err)
				return
			}

			select {
			case list <- mod:
			case <-ctx.Done():
				errs <- ErrSelectByID.WithReason(ctx.Err())
				return
			}
		}
	}()

	return list, errs
}
