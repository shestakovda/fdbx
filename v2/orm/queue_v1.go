package orm

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/models"
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
	var tsk *v1Task

	keys := make([]fdbx.Key, 0, 3*len(ids))

	for i := range ids {
		// Помечаем к удалению из неподтвержденных
		keys = append(keys, q.mgr.Wrap(ids[i].LPart(qWork)))

		// Помечаем к удалению из индекса статусов задач
		keys = append(keys, q.mgr.Wrap(ids[i].LPart(qMeta)))

		// Пока не удалили - загружаем задачу
		if tsk, err = q.loadTask(tx, ids[i]); err != nil {
			return ErrAck.WithReason(err)
		}

		// Если задача еще висит и в ней указано плановое время, можем грохнуть из плана
		if plan := tsk.Planned(); !plan.IsZero() {
			keys = append(keys, q.mgr.Wrap(ids[i].LPart(fdbx.Time2Byte(plan)...).LPart(qList)))
		}
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

func (q v1Queue) Pub(tx mvcc.Tx, key fdbx.Key, args ...Option) error {
	return q.PubList(tx, []fdbx.Key{key}, args...)
}

func (q v1Queue) PubList(tx mvcc.Tx, ids []fdbx.Key, args ...Option) (err error) {
	opts := getOpts(args)
	plan := time.Now().Add(opts.delay)
	when := fdbx.Time2Byte(plan)

	// Структура ключа:
	// db nsUser tb.id q.id qList delay uid = taskID
	pairs := make([]fdbx.Pair, 0, 2*len(ids))
	for i := range ids {
		task := q.newTask(ids[i], plan, &opts)

		pairs = append(pairs,
			// Основная запись таски (только айдишка, на которую триггеримся)
			fdbx.NewPair(q.mgr.Wrap(ids[i].LPart(when...).LPart(qList)), task.Key()),
			// Служебная запись в коллекцию метаданных
			fdbx.NewPair(q.mgr.Wrap(ids[i].LPart(qMeta)), task.Dump()),
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

func (q v1Queue) Sub(ctx context.Context, cn db.Connection, pack int) (<-chan Task, <-chan error) {
	res := make(chan Task)
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
			var list []Task

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

func (q v1Queue) SubList(ctx context.Context, cn db.Connection, pack int) (list []Task, err error) {
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

			// Достаем данные по каждой задаче, кроме тех, по которым исходный объект уже удален
			if list, e = q.loadTasks(tx, pairs, true); e != nil {
				return
			}

			// Уменьшаем счетчик задач в ожидании
			w.Increment(txmgr.Wrap(q.mgr.Wrap(qTotalWaitKey)), int64(-len(pairs)))

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

func (q v1Queue) Lost(tx mvcc.Tx, pack int) (list []Task, err error) {
	if pack == 0 {
		return nil, nil
	}

	var pairs []fdbx.Pair
	wkey := q.mgr.Wrap(fdbx.Key{qWork})

	if pairs, err = tx.ListAll(
		mvcc.To(wkey),
		mvcc.From(wkey),
		mvcc.Limit(pack),
	); err != nil {
		return nil, ErrLost.WithReason(err)
	}

	if len(pairs) == 0 {
		return nil, nil
	}

	if list, err = q.loadTasks(tx, pairs, false); err != nil {
		return nil, ErrLost.WithReason(err)
	}

	return list, nil
}

func (q v1Queue) Task(tx mvcc.Tx, key fdbx.Key) (res Task, err error) {
	var tsk *v1Task

	if tsk, err = q.loadTask(tx, key); err != nil {
		// Если метаданные задачи не найдены, значит считаем задачу подтвержденной
		if errx.Is(err, mvcc.ErrNotFound) {
			return q.confTask(key), nil
		}
		return nil, ErrTask.WithReason(err)
	}

	return tsk, nil
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

func (q v1Queue) onTaskWork(tx mvcc.Tx, p fdbx.Pair, w db.Writer) (err error) {
	var val []byte
	var key fdbx.Key
	var pair fdbx.Pair

	if key, err = p.Key(); err != nil {
		return ErrSub.WithReason(err)
	}

	// Удаление по ключу из основной очереди
	if err = tx.Delete([]fdbx.Key{key}, mvcc.Writer(w)); err != nil {
		return ErrSub.WithReason(err)
	}

	mkey := q.mgr.Wrap(q.mgr.Unwrap(key).LPart(qMeta))

	// Выборка элемента из коллекции метаданных
	if pair, err = tx.Select(mkey); err != nil {
		return ErrSub.WithReason(err)
	}

	// Получаем буфер метаданных
	if val, err = pair.Value(); err != nil {
		return ErrSub.WithReason(err)
	}

	// Целиком распаковывать буфер нам нет смысла, меняем только кол-во попыток и статус
	meta := models.GetRootAsTask(val, 0).State(nil)

	if !meta.MutateRepeats(meta.Repeats() + 1) {
		return ErrSub.WithStack()
	}

	if !meta.MutateStatus(StatusUnconfirmed) {
		return ErrSub.WithStack()
	}

	if err = tx.Upsert([]fdbx.Pair{
		p.WrapKey(q.wrkKeyWrapper), // Вставка в коллекцию задач "в работе"
		fdbx.NewPair(mkey, val),    // Вставка в коллекцию метаданных измененного буфера
	}, mvcc.Writer(w)); err != nil {
		return ErrSub.WithReason(err)
	}

	return nil
}

func (q v1Queue) loadTask(tx mvcc.Tx, key fdbx.Key) (tsk *v1Task, err error) {
	var buf []byte
	var sel fdbx.Pair

	// Выборка элемента из коллекции метаданных, если его нет - это ужасная ошибка
	if sel, err = tx.Select(q.mgr.Wrap(key.LPart(qMeta))); err != nil {
		return
	}

	// Получаем буфер метаданных
	if buf, err = sel.Value(); err != nil {
		return
	}

	// Распаковываем модель метаданных задачи
	tsk = &v1Task{q: q, m: models.GetRootAsTask(buf, 0).UnPack()}

	// Так мы достаем объект коллекции. Потенциально удаленный
	if sel, err = q.tb.Select(tx).PossibleByID(key).First(); err != nil {
		return
	}

	if sel != nil {
		if tsk.b, err = sel.Value(); err != nil {
			return
		}
	}

	return tsk, nil
}

func (q v1Queue) loadTasks(tx mvcc.Tx, items []fdbx.Pair, strict bool) (res []Task, err error) {
	var tsk *v1Task
	var key fdbx.Key

	res = make([]Task, 0, len(items))

	for i := range items {
		// Значение элемента - идентификатор объекта в коллекции
		if key, err = items[i].Value(); err != nil {
			return
		}

		// Получаем исходные данные объекта как данные задачи
		if tsk, err = q.loadTask(tx, key); err != nil {
			return
		}

		// Проверка на то, жив ли объект с исходными данными задачи
		if tsk.b == nil && strict {
			continue
		}

		res = append(res, tsk)
	}

	return res, nil
}

func (q v1Queue) newTask(key fdbx.Key, planned time.Time, opts *options) *v1Task {
	t := v1Task{
		q: q,
	}

	if opts.task != nil {
		t.m = opts.task
		t.m.State.Status = StatusPublished
		t.m.State.Planned = planned.UTC().UnixNano()
		return &t
	}

	t.m = &models.TaskT{
		Key: key.Bytes(),
		State: &models.TaskStateT{
			Status:  StatusPublished,
			Repeats: 0,
			Created: time.Now().UTC().UnixNano(),
			Planned: planned.UTC().UnixNano(),
		},
		Creator: opts.creator,
		Headers: make([]*models.TaskHeaderT, 0, len(opts.headers)),
	}

	for name, text := range opts.headers {
		t.m.Headers = append(t.m.Headers, &models.TaskHeaderT{
			Name: name,
			Text: text,
		})
	}

	return &t
}

func (q v1Queue) confTask(key fdbx.Key) *v1Task {
	t := v1Task{
		q: q,
		m: &models.TaskT{
			Key: key.Bytes(),
			State: &models.TaskStateT{
				Status: StatusConfirmed,
			},
		},
	}

	return &t
}

type v1Task struct {
	b []byte
	q v1Queue
	m *models.TaskT
}

func (t v1Task) Key() fdbx.Key { return t.m.Key }

func (t v1Task) Body() []byte { return t.b }

func (t v1Task) Dump() []byte { return fdbx.FlatPack(t.m) }

func (t v1Task) Status() byte { return t.m.State.Status }

func (t v1Task) Repeats() uint32 { return t.m.State.Repeats }

func (t v1Task) Creator() string { return t.m.Creator }

func (t v1Task) Created() time.Time { return time.Unix(0, t.m.State.Created) }

func (t v1Task) Planned() time.Time { return time.Unix(0, t.m.State.Planned) }

func (t v1Task) Headers() map[string]string {
	res := make(map[string]string, len(t.m.Headers))

	for i := range t.m.Headers {
		res[t.m.Headers[i].Name] = t.m.Headers[i].Text
	}

	return res
}

func (t v1Task) Ack(tx mvcc.Tx) error { return t.q.Ack(tx, t.m.Key) }

func (t v1Task) Repeat(tx mvcc.Tx, d time.Duration) (err error) {
	if err = t.Ack(tx); err != nil {
		return
	}

	return t.q.Pub(tx, t.m.Key, metatask(t.m), Delay(d))
}
