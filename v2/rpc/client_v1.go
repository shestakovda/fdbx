package rpc

import (
	"context"
	"time"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/fdbx/v2/orm"
	"github.com/shestakovda/typex"
)

func newClientV1(cn db.Connection, srvID uint16) Client {
	c := v1Client{
		conn: cn,
		data: orm.NewTable(srvID),
	}

	return &c
}

type v1Client struct {
	data orm.Table
	conn db.Connection
}

func (c v1Client) SyncExec(ctx context.Context, endID uint16, data []byte) (val []byte, err error) {
	var tx mvcc.Tx
	var pair fdbx.Pair
	var waiter fdbx.Waiter

	queue := orm.NewQueue(endID, c.data)

	if tx, err = mvcc.Begin(c.conn); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}
	defer tx.Cancel()

	key := fdbx.Key(typex.NewUUID())
	req := key.RPart(NSRequest)
	res := key.RPart(NSResponse)

	if err = c.data.Upsert(tx, fdbx.NewPair(req, data)); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	if err = queue.Pub(tx, time.Now(), req); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	wkey := mvcc.NewTxKeyManager().Wrap(orm.NewWatchKeyManager(c.data.ID()).Wrap(req))
	defer func() { c.conn.Write(func(w db.Writer) error { w.Delete(wkey); return nil }) }()

	if err = c.conn.Write(func(w db.Writer) error {
		w.Increment(wkey, 1)
		waiter = w.Watch(wkey)
		return nil
	}); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	if err = tx.Commit(); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	wctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	if err = waiter.Resolve(wctx); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	if tx, err = mvcc.Begin(c.conn); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}
	defer tx.Cancel()

	if pair, err = c.data.Select(tx).ByID(res).First(); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	if val, err = pair.Value(); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	return val, nil
}
