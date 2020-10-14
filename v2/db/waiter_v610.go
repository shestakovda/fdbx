package db

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type v610Waiter struct {
	fdb.FutureNil
}

func (w v610Waiter) Resolve(ctx context.Context) (err error) {
	wc := make(chan error, 1)

	go func() {
		defer close(wc)

		if exp := w.Get(); exp != nil {
			wc <- ErrWait.WithReason(exp)
		}
	}()

	select {
	case err = <-wc:
		return
	case <-ctx.Done():
		w.Cancel()
		return ErrWait.WithReason(ctx.Err())
	}
}
