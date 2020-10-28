package orm

import (
	"context"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func newBaseSelector(tx mvcc.Tx) *baseSelector {
	s := baseSelector{
		tx: tx,
	}
	return &s
}

type baseSelector struct {
	tx mvcc.Tx
	lk fdbx.Key
}

func (s baseSelector) LastKey() fdbx.Key { return s.lk }

func (s *baseSelector) setKey(pair fdbx.Pair) (err error) {
	if s.lk, err = pair.Key(); err != nil {
		return ErrSelect.WithReason(err)
	}

	return nil
}

func (s *baseSelector) sendPair(ctx context.Context, list chan fdbx.Pair, pair fdbx.Pair) (err error) {
	select {
	case list <- pair:
		if s.lk, err = pair.Key(); err != nil {
			return ErrSelect.WithReason(err)
		}
	case <-ctx.Done():
		return ErrSelect.WithReason(ctx.Err())
	}
	return nil
}
