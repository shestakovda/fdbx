package orm

import (
	"context"
	"sync/atomic"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func newBaseSelector(tx mvcc.Tx) *baseSelector {
	return &baseSelector{
		tx: tx,
	}
}

type baseSelector struct {
	tx mvcc.Tx
	lk atomic.Value
}

func (s *baseSelector) LastKey() fdbx.Key {
	return s.lk.Load().(fdbx.Key)
}

func (s *baseSelector) setKey(key fdbx.Key) {
	s.lk.Store(key)
}

func (s *baseSelector) sendPair(ctx context.Context, list chan fdbx.Pair, pair fdbx.Pair) (err error) {
	select {
	case list <- pair:
	case <-ctx.Done():
		return ErrSelect.WithReason(ctx.Err())
	}

	return nil
}
