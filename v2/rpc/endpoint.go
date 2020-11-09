package rpc

import (
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/fdbx/v2/orm"
)

func newEndpoint(id uint16, tbl orm.Table, hdl TaskHandler, args []Option) *endpoint {
	opts := getOpts(args)

	e := endpoint{
		Queue:    orm.NewQueue(id, tbl, orm.Refresh(opts.refresh)),
		Async:    opts.async,
		OnTask:   hdl,
		OnError:  opts.onError,
		OnListen: opts.onListen,
	}

	return &e
}

type endpoint struct {
	Async    bool
	Queue    orm.Queue
	OnTask   TaskHandler
	OnError  ErrorHandler
	OnListen ListenHandler
}

func (e endpoint) check() (err error) {
	if e.Queue == nil {
		return ErrBadListener.WithDetail("Отсутствует очередь задач")
	}

	if e.OnListen == nil {
		return ErrBadListener.WithDetail("Отсутствует обработчик ошибки подписки")
	}

	if e.OnTask == nil {
		return ErrBadListener.WithDetail("Отсутствует обработчик задачи")
	}

	return nil
}

func (e endpoint) repeat(cn db.Connection, task orm.Task, wait time.Duration) (err error) {
	var tx mvcc.Tx

	// Создаем транзакцию для повтора
	if tx, err = mvcc.Begin(cn); err != nil {
		return ErrRepeat.WithReason(err)
	}
	defer tx.Cancel()

	// Вызываем повтор. Это увеличит счетчики и оставит все заголовки
	if err = task.Repeat(tx, wait); err != nil {
		return ErrRepeat.WithReason(err)
	}

	// Подтверждаем транзакцию
	if err = tx.Commit(); err != nil {
		return ErrRepeat.WithReason(err)
	}

	return nil
}

func (e endpoint) answer(cn db.Connection, tbl orm.Table, task orm.Task, res []byte, exp error) (err error) {

	// Если это асинхронный вызов, то можно только подтвердить задачу, ответ должен быть сформирован в обработчике
	if e.Async {
		return e.ack(cn, task, exp)
	}

	var tx mvcc.Tx

	// Из исходного ключа задачи сформируем симметричный ответный ключ
	key := task.Key()
	cfe := ErrConfirm
	ans := &models.AnswerT{
		Err: false,
		Buf: res,
	}

	// Если обработка завершилась с ошибкой, упаковываем её в структуру
	if exp != nil {
		ans = e.errAnswer(exp)

		// Если при подтверждении ошибки будет фейл, надо чтобы ошибка обработки не потерялась
		cfe = cfe.WithReason(exp)
	}

	// Новая транзакция, в которой мы опубликуем ответ и подтвердим задачу
	if tx, err = mvcc.Begin(cn); err != nil {
		return cfe.WithReason(err)
	}
	defer tx.Cancel()

	// Вставляем в таблицу объект с ответом или ошибкой
	if err = tbl.Upsert(tx, fdbx.NewPair(key.Clone().RSkip(1).RPart(NSResponse), fdbx.FlatPack(ans))); err != nil {
		return cfe.WithReason(err)
	}

	// Если не было ошибки обработки, можем подтвердить задачу
	if exp == nil {
		if err = task.Ack(tx); err != nil {
			return cfe.WithReason(err)
		}
	}

	// Сначала надо сделать коммит транзакции с ответом, чтобы он стал виден остальным
	if err = tx.Commit(); err != nil {
		return cfe.WithReason(err)
	}

	// Формируем данные для подтверждения ключа ожидания
	_, wnow := waits(tbl.ID(), key)

	if err = cn.Write(func(w db.Writer) error { return w.Upsert(wnow) }); err != nil {
		return cfe.WithReason(err)
	}

	return nil
}

func (e endpoint) ack(cn db.Connection, task orm.Task, exp error) (err error) {

	// В случае ошибки, подтверждать задачу нельзя
	if exp != nil {
		return nil
	}

	var tx mvcc.Tx

	if tx, err = mvcc.Begin(cn); err != nil {
		return ErrConfirm.WithReason(err)
	}
	defer tx.Cancel()

	if err = task.Ack(tx); err != nil {
		return ErrConfirm.WithReason(err)
	}

	if err = tx.Commit(); err != nil {
		return ErrConfirm.WithReason(err)
	}

	return nil
}

func (e endpoint) errAnswer(err error) *models.AnswerT {
	var ok bool
	var exp errx.Error

	// Сначала нужно преобразовать её к нужному типу
	if exp, ok = err.(errx.Error); !ok {
		exp = errx.ErrInternal.WithReason(err)
	}

	// Формируем ответную структуру с буфером ошибки
	return &models.AnswerT{
		Err: true,
		Buf: exp.Pack(),
	}
}

func waits(tbid uint16, key fdbx.Key) (fdbx.Key, fdbx.Pair) {
	wkey := mvcc.NewTxKeyManager().Wrap(orm.NewWatchKeyManager(tbid).Wrap(key))
	wnow := fdbx.NewPair(wkey, fdbx.Time2Byte(time.Now()))
	return wkey, wnow
}
