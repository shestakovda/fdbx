package rpc

import (
	"time"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/fdbx/v2/orm"
)

func newEndpoint(id uint16, tbl orm.Table, hdl TaskHandler, args []Option) *endpoint {
	opts := getOpts(args)

	e := endpoint{
		Queue:    orm.NewQueue(id, tbl, orm.Refresh(opts.refresh)),
		AsRPC:    opts.asRPC,
		OnTask:   hdl,
		OnError:  opts.onError,
		OnListen: opts.onListen,
	}

	return &e
}

type endpoint struct {
	AsRPC    bool
	Queue    orm.Queue
	OnTask   TaskHandler
	OnError  ErrorHandler
	OnListen ErrorHandler
}

func (e endpoint) check() (err error) {
	if e.Queue == nil {
		return ErrBadListener.WithDetail("Отсутствует очередь задач")
	}

	if e.OnListen == nil {
		return ErrBadListener.WithDetail("Отсутствует обработчик ошибки подписки")
	}

	if e.OnError == nil {
		return ErrBadListener.WithDetail("Отсутствует обработчик ошибки задачи")
	}

	if e.OnTask == nil {
		return ErrBadListener.WithDetail("Отсутствует обработчик задачи")
	}

	return nil
}

func (e endpoint) repeat(cn db.Connection, pair fdbx.Pair, wait time.Duration) (err error) {
	var tx mvcc.Tx
	var key fdbx.Key

	if key, err = pair.Key(); err != nil {
		return ErrRepeat.WithReason(err)
	}

	if tx, err = mvcc.Begin(cn); err != nil {
		return ErrRepeat.WithReason(err)
	}
	defer tx.Cancel()

	if err = e.Queue.Pub(tx, time.Now().Add(wait), key); err != nil {
		return ErrRepeat.WithReason(err)
	}

	if err = tx.Commit(); err != nil {
		return ErrRepeat.WithReason(err)
	}

	return nil
}

func (e endpoint) confirm(cn db.Connection, tbl orm.Table, pair fdbx.Pair, res []byte) (err error) {
	var tx mvcc.Tx
	var key fdbx.Key

	if key, err = pair.Key(); err != nil {
		return ErrConfirm.WithReason(err)
	}

	if tx, err = mvcc.Begin(cn); err != nil {
		return ErrConfirm.WithReason(err)
	}
	defer tx.Cancel()

	if e.AsRPC {
		if err = tbl.Upsert(tx, fdbx.NewPair(key.Clone().RSkip(1).RPart(NSResponse), res)); err != nil {
			return ErrConfirm.WithReason(err)
		}
	}

	if err = e.Queue.Ack(tx, key); err != nil {
		return ErrConfirm.WithReason(err)
	}

	if err = tx.Commit(); err != nil {
		return ErrConfirm.WithReason(err)
	}

	if e.AsRPC {
		wkey := mvcc.NewTxKeyManager().Wrap(orm.NewWatchKeyManager(tbl.ID()).Wrap(key))
		wnow := fdbx.NewPair(wkey, fdbx.Time2Byte(time.Now()))

		if err = cn.Write(func(w db.Writer) error { return w.Upsert(wnow) }); err != nil {
			return ErrConfirm.WithReason(err)
		}
	}

	return nil
}
