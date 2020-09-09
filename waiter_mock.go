package fdbx

import "context"

type mockWaiter struct {
	*MockConn
}

func (w *mockWaiter) Wait(ctx context.Context, fnc WaitCallback, ids ...string) error {
	return w.FWait(ctx, fnc, ids...)
}
