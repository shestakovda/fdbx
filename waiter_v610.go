package fdbx

import (
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func newV610Waiter(conn *v610Conn, typeID uint16) *v610waiter {
	return &v610waiter{
		conn: conn,
		tpid: typeID,
	}
}

type v610waiter struct {
	tpid uint16
	conn *v610Conn
	exit chan struct{}
}

func (w *v610waiter) Wait(ctx context.Context, fnc WaitCallback, ids ...string) error {
	waits := make([]*v610wait, len(ids))

	if _, err := w.conn.fdb.Transact(func(tx fdb.Transaction) (interface{}, error) {
		for i := range ids {
			waits[i] = &v610wait{id: ids[i], fnc: fnc, future: tx.Watch(w.key(ids[i])), parent: w}
		}
		return nil, nil
	}); err != nil {
		return err
	}

	for i := range waits {
		go waits[i].wait(ctx)
	}

	return nil
}

func (w *v610waiter) key(id string) fdb.Key {
	return fdbKey(w.conn.db, w.tpid, S2B(id))
}

type v610wait struct {
	id     string
	fnc    WaitCallback
	future fdb.FutureNil
	parent *v610waiter
}

func (w *v610wait) wait(ctx context.Context) {
	ch := make(chan struct{}, 1)

	go func() {
		defer close(ch)
		defer w.future.Cancel()
		w.future.BlockUntilReady()
		ch <- struct{}{}
	}()

	select {
	case <-ctx.Done():
		w.future.Cancel()
		return
	case <-w.parent.exit:
		w.future.Cancel()
		return
	case <-ch:
		if w.fnc(w.id) {
			w.restart(ctx)
		}
	}
}

func (w *v610wait) restart(ctx context.Context) {
	w.parent.conn.fdb.Transact(func(tx fdb.Transaction) (interface{}, error) {
		w.future = tx.Watch(w.parent.key(w.id))
		go w.wait(ctx)
		return nil, nil
	})
}
