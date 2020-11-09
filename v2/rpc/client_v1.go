package rpc

import (
	"context"

	"github.com/golang/glog"
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/models"
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

func (c v1Client) SyncExec(ctx context.Context, endID uint16, data []byte, args ...Option) (val []byte, err error) {
	var tx mvcc.Tx
	var waiter fdbx.Waiter

	opts := getOpts(args)
	queue := orm.NewQueue(endID, c.data)

	if tx, err = mvcc.Begin(c.conn); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}
	defer tx.Cancel()

	key := fdbx.Key(typex.NewUUID())
	req := key.RPart(NSRequest)
	rep := fdbx.NewPair(req, data)

	if err = c.data.Upsert(tx, rep); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	if err = queue.Pub(tx, req); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	wkey, wnow := waits(c.data.ID(), req)
	defer func() { c.conn.Write(func(w db.Writer) error { w.Delete(wkey); return nil }) }()

	// Важно выставить ожидание по ключу раньше, чем заккомитим транзакцию
	// Чтобы не проворонить результат обработчика, который может сработать оч быстро
	if err = c.conn.Write(func(w db.Writer) (exp error) {
		if exp = w.Upsert(wnow); exp != nil {
			return
		}
		waiter = w.Watch(wkey)
		return nil
	}); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	if err = tx.Commit(); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	// В любом случае, подчищаем за собой данные по задаче
	defer c.clean(key)

	wctx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()

	if err = waiter.Resolve(wctx); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	if val, err = c.Result(key); err != nil {
		return nil, ErrSyncExec.WithReason(err)
	}

	return val, nil
}

// По сути ошибка очистки ни на что не влияет, поэтому просто принтим ее
// Не возвращаем, потому что все равно метод вызывается в defer, это никому не нужно
func (c v1Client) clean(key fdbx.Key) {
	var err error
	var tx mvcc.Tx

	defer func() {
		if err != nil {
			glog.Errorf("%+v", ErrClean.WithReason(err))
		}
	}()

	if tx, err = mvcc.Begin(c.conn); err != nil {
		return
	}
	defer tx.Cancel()

	// Подчищаем задачу, чтобы её больше никто не выполнял, а также ответ, если он был
	if err = c.data.Delete(tx, key.RPart(NSRequest), key.RPart(NSResponse)); err != nil {
		return
	}

	if err = tx.Commit(); err != nil {
		return
	}
}

func (c v1Client) Result(key fdbx.Key) (val []byte, err error) {
	var tx mvcc.Tx
	var pair fdbx.Pair

	if tx, err = mvcc.Begin(c.conn); err != nil {
		return nil, ErrResult.WithReason(err)
	}
	defer tx.Cancel()

	if pair, err = c.data.Select(tx).ByID(key.RPart(NSResponse)).First(); err != nil {
		return nil, ErrResult.WithReason(err)
	}

	if val, err = pair.Value(); err != nil {
		return nil, ErrResult.WithReason(err)
	}

	if len(val) == 0 {
		return nil, ErrResult.WithStack()
	}

	ans := models.GetRootAsAnswer(val, 0).UnPack()

	if ans.Err {
		return nil, errx.Unpack(ans.Buf)
	}

	return ans.Buf, nil
}
