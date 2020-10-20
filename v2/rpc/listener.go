package rpc

import (
	"time"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func (l Listener) check() (err error) {
	if l.Queue == nil {
		return ErrBadListener.WithDetail("Отсутствует очередь задач")
	}

	if l.OnListen == nil {
		return ErrBadListener.WithDetail("Отсутствует обработчик ошибки подписки")
	}

	if l.OnError == nil {
		return ErrBadListener.WithDetail("Отсутствует обработчик ошибки задачи")
	}

	if l.OnTask == nil {
		return ErrBadListener.WithDetail("Отсутствует обработчик задачи")
	}

	return nil
}

func (l Listener) repeat(cn db.Connection, pair fdbx.Pair, wait time.Duration) (err error) {
	var tx mvcc.Tx
	var key fdbx.Key

	if key, err = pair.Key(); err != nil {
		return
	}

	if tx, err = mvcc.Begin(cn); err != nil {
		return
	}
	defer tx.Cancel()

	if err = l.Queue.Pub(tx, time.Now().Add(wait), key); err != nil {
		return
	}

	return tx.Commit()
}

func (l Listener) ack(cn db.Connection, pair fdbx.Pair) (err error) {
	var tx mvcc.Tx
	var key fdbx.Key

	if key, err = pair.Key(); err != nil {
		return
	}

	if tx, err = mvcc.Begin(cn); err != nil {
		return
	}
	defer tx.Cancel()

	if err = l.Queue.Ack(tx, key); err != nil {
		return
	}

	return tx.Commit()
}
