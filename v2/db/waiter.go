package db

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

type Waiter struct {
	fdb.FutureNil
}

func (w *Waiter) Clear() {
	if w.FutureNil != nil && !w.IsReady() {
		w.Cancel()
	}
	w.FutureNil = nil
}

func (w *Waiter) Empty() bool { return w.FutureNil == nil }

func (w *Waiter) Resolve(ctx context.Context) (err error) {
	if w.Empty() {
		return ErrWait.WithStack()
	}

	wc := make(chan error, 1)
	defer w.Clear()

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
		return ErrWait.WithReason(ctx.Err())
	}
}
