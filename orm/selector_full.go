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

func (s *fullSelector) Select(ctx context.Context, cl Collection) (<-chan Model, <-chan error) {
	list := make(chan Model)
	errs := make(chan error, 1)

	go func() {
		var err error

		defer close(list)
		defer close(errs)

		rng := &mvcc.Range{
			From: mvcc.NewBytesKey([]byte{0x00}),
			To:   mvcc.NewBytesKey([]byte{0xFF}),
		}

		// pairs, errc := s.tx.FullScan(ctx, rng, s.cnt)
		pairs, errc := s.tx.SeqScan(ctx, rng)

		for pair := range pairs {
			mod := cl.Fabric()(cl.UsrKey(pair.Key))

			if err = mod.Unpack(pair.Value); err != nil {
				errs <- ErrSelectFull.WithReason(err)
				return
			}

			select {
			case list <- mod:
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
