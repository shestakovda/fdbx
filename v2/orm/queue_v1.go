package orm

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewQueue(id uint16, tb Table, args ...Option) Queue {
	opts := getOpts(args)
	q := v1Queue{
		id:      id,
		tb:      tb,
		options: opts,
		mgr:     newQueueKeyManager(tb.ID(), id, opts.prefix),
	}

	return &q
}

type v1Queue struct {
	options

	id  uint16
	tb  Table
	mgr fdbx.KeyManager
}

func (q v1Queue) ID() uint16 { return q.id }

func (q v1Queue) Ack(tx mvcc.Tx, ids ...fdbx.Key) (err error) {
	keys := make([]fdbx.Key, 2*len(ids))

	for i := range ids {
		// Удаляем из неподтвержденных
		keys[2*i] = q.mgr.Wrap(ids[i].LPart(qWork))
		// Удаляем из индекса статусов задач
		keys[2*i+1] = q.mgr.Wrap(ids[i].LPart(qStat))
	}

	if err = tx.Delete(keys); err != nil {
		return ErrAck.WithReason(err)
	}

	tx.OnCommit(func(w db.Writer) error {
		txmgr := mvcc.NewTxKeyManager()
		// Уменьшаем счетчик задач в работе
		w.Increment(txmgr.Wrap(q.mgr.Wrap(qTotalWorkKey)), int64(-len(ids)))
		return nil
	})
	return nil
}

func (q v1Queue) Pub(tx mvcc.Tx, when time.Time, ids ...fdbx.Key) (err error) {

	if when.IsZero() {
		when = time.Now()
	}

	delay := fdbx.Time2Byte(when)

	// Структура ключа:
	// db nsUser tb.id q.id qList delay uid = taskID
	pairs := make([]fdbx.Pair, 2*len(ids))
	for i := range ids {
		// Основная запись таски
		pairs[2*i] = fdbx.NewPair(
			// Случайная айдишка таски, чтобы не было конфликтов при одинаковом времени
			q.mgr.Wrap(ids[i].LPart(delay...).LPart(qList)),
			// Айдишку элемента очереди записываем в значение, именно она и является таской
			[]byte(ids[i]),
		)

		// Служебная запись в индекс состояний
		pairs[2*i+1] = fdbx.NewPair(
			q.mgr.Wrap(ids[i].LPart(qStat)),
			[]byte{StatusPublished},
		)
	}

	if err = tx.Upsert(pairs); err != nil {
		return ErrPub.WithReason(err)
	}

	// Особая магия - инкремент счетчика очереди, чтобы затриггерить подписчиков
	// А также инкремент счетчиков статистики очереди
	tx.OnCommit(func(w db.Writer) error {
		txmgr := mvcc.NewTxKeyManager()
		// Увеличиваем счетчик задач в ожидании
		w.Increment(txmgr.Wrap(q.mgr.Wrap(qTotalWaitKey)), int64(len(ids)))
		// Триггерим обработчики забрать новые задачи
		w.Increment(txmgr.Wrap(q.mgr.Wrap(qTriggerKey)), 1)
		return nil
	})

	return nil
}

func (q v1Queue) Sub(ctx context.Context, cn db.Connection, pack int) (<-chan fdbx.Pair, <-chan error) {
	res := make(chan fdbx.Pair)
	errc := make(chan error, 1)

	go func() {

		defer close(errc)
		defer close(res)
		defer func() {
			if rec := recover(); rec != nil {
				if err, ok := rec.(error); ok {
					errc <- ErrSub.WithReason(err)
				} else {
					errc <- ErrSub.WithReason(fmt.Errorf("%+v", rec))
				}
			}
		}()

		hdlr := func() (err error) {
			var list []fdbx.Pair

			if list, err = q.SubList(ctx, cn, pack); err != nil {
				return
			}

			if len(list) == 0 {
				return
			}

			for i := range list {
				select {
				case res <- list[i]:
				case <-ctx.Done():
					return ErrSub.WithReason(ctx.Err())
				}
			}

			return nil
		}

		for {
			if err := hdlr(); err != nil {
				errc <- err
				return
			}
		}
	}()

	return res, errc
}

func (q v1Queue) SubList(ctx context.Context, cn db.Connection, pack int) (list []fdbx.Pair, err error) {
	if pack == 0 {
		return nil, nil
	}

	var pairs []fdbx.Pair
	var waiter fdbx.Waiter

	from := q.mgr.Wrap(fdbx.Key{qList})
	hdlr := func() (exp error) {
		var tx mvcc.Tx

		to := q.mgr.Wrap(fdbx.Key(fdbx.Time2Byte(time.Now())).LPart(qList))

		if tx, err = mvcc.Begin(cn); err != nil {
			return ErrSub.WithReason(err)
		}
		defer tx.Cancel()

		// Критически важно делать это в одной физической транзакции
		// Иначе остается шанс, что одну и ту же задачу возьмут в обработку два воркера
		return tx.Conn().Write(func(w db.Writer) (e error) {
			txmgr := mvcc.NewTxKeyManager()

			if pairs, e = tx.ListAll(
				mvcc.To(to),
				mvcc.From(from),
				mvcc.Limit(pack),
				mvcc.Exclusive(q.onTaskWork),
				mvcc.Writer(w),
			); e != nil {
				return
			}

			if len(pairs) == 0 {
				// В этом случае не коммитим, т.к. по сути ничего не изменилось
				waiter = w.Watch(txmgr.Wrap(q.mgr.Wrap(qTriggerKey)))
				return nil
			}

			ids := make([]fdbx.Key, len(pairs))

			for i := range pairs {
				if ids[i], e = pairs[i].Value(); e != nil {
					return
				}

			}

			if list, e = q.tb.Select(tx).PossibleByID(ids...).All(); e != nil {
				return
			}

			// Уменьшаем счетчик задач в ожидании
			w.Increment(txmgr.Wrap(q.mgr.Wrap(qTotalWaitKey)), int64(-len(ids)))

			// Увеличиваем счетчик задач в ожидании
			w.Increment(txmgr.Wrap(q.mgr.Wrap(qTotalWorkKey)), int64(len(list)))

			// Логический коммит в той же физической транзакции
			// Это самый важный момент - именно благодаря этому перемещенные в процессе чтения
			// элементы очереди будут видны как перемещенные для других логических транзакций
			return tx.Commit(mvcc.Writer(w))
		})
	}

	for {
		if err = q.waitTask(ctx, waiter); err != nil {
			return
		}

		if err = hdlr(); err != nil {
			return nil, ErrSub.WithReason(err)
		}

		if len(list) > 0 {
			return list, nil
		}
	}
}

func (q v1Queue) Lost(tx mvcc.Tx, pack int) (list []fdbx.Pair, err error) {
	if pack == 0 {
		return nil, nil
	}

	var id []byte
	var pairs []fdbx.Pair

	key := q.mgr.Wrap(fdbx.Key{qWork})

	// Значения в этих парах - айдишки элементов коллекции
	if pairs, err = tx.ListAll(mvcc.From(key), mvcc.Limit(pack)); err != nil {
		return nil, ErrLost.WithReason(err)
	}

	if len(pairs) == 0 {
		return nil, nil
	}

	ids := make([]fdbx.Key, len(pairs))

	for i := range pairs {
		if id, err = pairs[i].Value(); err != nil {
			return nil, ErrLost.WithReason(err)
		}
		ids[i] = fdbx.Key(id)
	}

	if list, err = q.tb.Select(tx).PossibleByID(ids...).All(); err != nil {
		return nil, ErrLost.WithReason(err)
	}

	return list, nil
}

func (q v1Queue) Status(tx mvcc.Tx, ids ...fdbx.Key) (res map[string]byte, err error) {
	var val []byte
	var pair fdbx.Pair

	res = make(map[string]byte, len(ids))

	for i := range ids {
		status := StatusConfirmed

		if pair, err = tx.Select(q.mgr.Wrap(ids[i].LPart(qStat))); err == nil {
			if val, err = pair.Value(); err != nil {
				return nil, ErrStatus.WithReason(err)
			}
			if len(val) > 0 {
				status = val[0]
			}
		}

		res[ids[i].String()] = status
	}

	return res, nil
}

func (q v1Queue) Stat(tx mvcc.Tx) (wait, work int64, err error) {
	if err = tx.Conn().Read(func(r db.Reader) (exp error) {
		var val []byte

		txmgr := mvcc.NewTxKeyManager()

		if val, exp = r.Data(txmgr.Wrap(q.mgr.Wrap(qTotalWaitKey))).Value(); exp != nil {
			return
		}
		if len(val) == 8 {
			wait = int64(binary.LittleEndian.Uint64(val))
		}

		if val, exp = r.Data(txmgr.Wrap(q.mgr.Wrap(qTotalWorkKey))).Value(); exp != nil {
			return
		}
		if len(val) == 8 {
			work = int64(binary.LittleEndian.Uint64(val))
		}
		return nil
	}); err != nil {
		return 0, 0, ErrStat.WithReason(err)
	}

	return wait, work, nil
}

func (q v1Queue) waitTask(ctx context.Context, waiter fdbx.Waiter) (err error) {
	if waiter == nil {
		return nil
	}

	if ctx.Err() != nil {
		return ErrSub.WithReason(ctx.Err())
	}

	// Даже если waiter установлен, то при отсутствии других публикаций мы тут зависнем навечно.
	// А задачи, время которых настало, будут просрочены. Для этого нужен особый механизм обработки по таймауту.
	wctx, cancel := context.WithTimeout(ctx, q.options.refresh)
	defer cancel()

	if err = waiter.Resolve(wctx); err != nil {
		// Если запущено много обработчиков, все они рванут забирать события одновременно.
		// Чтобы избежать массовых конфликтов транзакций и улучшить распределение задач делаем небольшую
		// случайную задержку, в пределах 20 мс. Немного для человека, значительно для уменьшения конфликтов
		time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
	}

	return nil
}

func (q v1Queue) wrkKeyWrapper(key fdbx.Key) (fdbx.Key, error) {
	return q.mgr.Wrap(q.mgr.Unwrap(key).LPart(qWork)), nil
}

func (q v1Queue) onTaskWork(tx mvcc.Tx, p fdbx.Pair, w db.Writer) (exp error) {
	var key fdbx.Key

	if key, exp = p.Key(); exp != nil {
		return ErrSub.WithReason(exp)
	}

	// Удаление по ключу из основной очереди
	if exp = tx.Delete([]fdbx.Key{key}, mvcc.Writer(w)); exp != nil {
		return ErrSub.WithReason(exp)
	}

	pairs := []fdbx.Pair{
		// Вставка в коллекцию задач "в работе"
		p.WrapKey(q.wrkKeyWrapper),
		// Вставка в индекс статусов задач
		fdbx.NewPair(q.mgr.Wrap(q.mgr.Unwrap(key).LPart(qStat)), []byte{StatusUnconfirmed}),
	}

	if key, exp = pairs[0].Key(); exp != nil {
		return ErrSub.WithReason(exp)
	}

	if exp = tx.Upsert(pairs, mvcc.Writer(w)); exp != nil {
		return ErrSub.WithReason(exp)
	}

	return nil
}
