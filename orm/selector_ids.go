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

func (s *idsSelector) Select(ctx context.Context, cl Collection) (<-chan Row, <-chan error) {
	list := make(chan Row)
	errs := make(chan error, 1)

	go func() {
		var value mvcc.Value

		defer close(list)
		defer close(errs)

		// TODO: параллельная или массовая загрузка
		for i := range s.ids {
			select {
			case list <- cl.NewRow(s.ids[i], value):
			case <-ctx.Done():
				errs <- ErrSelectByID.WithReason(ctx.Err())
				return
			}
		}
	}()

	return list, errs
}
