package db

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type Waiter interface {
	Clear()
	Resolve(ctx context.Context) (err error)
}

type keyWaiter struct {
	fdb.FutureNil
}

func (w *keyWaiter) Clear() {
	if !w.IsReady() {
		w.Cancel()
	}
}

func (w *keyWaiter) Resolve(ctx context.Context) (err error) {
	wc := make(chan error, 1)
	defer w.Clear()

	go func() {
		defer close(wc)

		if !w.IsReady() {
			if exp := w.Get(); exp != nil {
				wc <- ErrWait.WithReason(exp)
			}
		}
	}()

	select {
	case err = <-wc:
		return
	case <-ctx.Done():
		return ErrWait.WithReason(ctx.Err())
	}
}
