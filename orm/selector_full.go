package orm

import (
	"context"

	"github.com/shestakovda/fdbx/mvcc"
)

func NewFullSelector(tx mvcc.Tx, cnt int) Selector {
	return &fullSelector{
		tx:  tx,
		cnt: cnt,
	}
}

type fullSelector struct {
	tx  mvcc.Tx
	cnt int
}

func (s *fullSelector) Select(ctx context.Context, cl Collection) (<-chan Row, <-chan error) {
	list := make(chan Row)
	errs := make(chan error, 1)

	go func() {
		var err error

		defer close(list)
		defer close(errs)

		rng := &mvcc.Range{
			From: cl.SysKey(mvcc.NewBytesKey([]byte{0x00})),
			To:   cl.SysKey(mvcc.NewBytesKey([]byte{0xFF})),
		}

		pairs, errc := s.tx.SeqScan(ctx, rng)

		for pair := range pairs {
			select {
			case list <- &row{key: cl.UsrKey(pair.Key), val: pair.Value, fab: cl.Fabric()}:
			case <-ctx.Done():
				errs <- ErrSelectFull.WithReason(ctx.Err())
				return
			}
		}

		for err = range errc {
			errs <- ErrSelectFull.WithReason(err)
			return
		}
	}()

	return list, errs
}
