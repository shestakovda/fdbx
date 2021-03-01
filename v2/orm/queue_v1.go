package orm

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/errx"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewQueue(id uint16, tb Table, args ...Option) Queue {
	return &v1Queue{
		id:      id,
		tb:      tb,
		options: getOpts(args),
	}
}

type v1Queue struct {
	options

	id uint16
	tb Table
}

func (q v1Queue) wrapFlagKey(flag byte, key fdb.Key) fdb.Key {
	return WrapQueueKey(q.tb.ID(), q.id, q.options.prefix, flag, key)
}

func (q v1Queue) wrapItemKey(plan time.Time, key fdb.Key) fdb.Key {
	return WrapQueueKey(q.tb.ID(), q.id, q.options.prefix, qList, fdbx.AppendLeft(key, fdbx.Time2Byte(plan)...))
}

func (q v1Queue) ID() uint16 { return q.id }

func (q v1Queue) Ack(tx mvcc.Tx, ids ...fdb.Key) (err error) {

	// Подтверждение задач надо делать атомарно, иначе может быть нарушена консистентность счетчиков
	if err = tx.Conn().Write(func(w db.Writer) (exp error) {
		var sel fdb.KeyValue

		keys := make([]fdb.Key, 0, 2*len(ids))
		diff := make(map[string]struct{}, len(ids))

		for i := range ids {
			mkey := q.wrapFlagKey(qMeta, ids[i])

			// Пока не удалили - загружаем задачу, если она есть то счетчик надо уменьшить
			if sel, exp = tx.Select(mkey, mvcc.Writer(w), mvcc.Lock()); exp == nil {
				if tsk := models.GetRootAsTask(sel.Value, 0).State(nil).UnPack(); tsk.Status == StatusUnconfirmed {
					diff[ids[i].String()] = struct{}{}

					// Помечаем к удалению из индекса статусов задач
					keys = append(keys, mkey)

					// Помечаем к удалению из неподтвержденных
					keys = append(keys, q.wrapFlagKey(qWork, ids[i]))
				}
			}
		}

		// Удаление всех ключей
		if exp = tx.Delete(keys, mvcc.Writer(w)); exp != nil {
			return
		}
		return nil
	}); err != nil {
		return ErrAck.WithReason(err)
	}

	return nil
}

func (q v1Queue) Pub(tx mvcc.Tx, key fdb.Key, args ...Option) error {
	return q.PubList(tx, []fdb.Key{key}, args...)
}

func (q v1Queue) PubList(tx mvcc.Tx, ids []fdb.Key, args ...Option) (err error) {
	opts := getOpts(args)
	plan := time.Now().Add(opts.delay)
	diff := make(map[string]struct{}, len(ids))

	// Структура ключа:
	// db nsUser tb.id q.id qList delay uid = taskID
	pairs := make([]fdb.KeyValue, 0, 2*len(ids))
	for i := range ids {
		task := q.newTask(ids[i], plan, &opts)
		diff[task.Key().String()] = struct{}{}
		pairs = append(pairs,
			fdb.KeyValue{Key: q.wrapFlagKey(qMeta, task.Key()), Value: task.Dump()},
			fdb.KeyValue{Key: q.wrapItemKey(plan, task.Key()), Value: task.Key()},
		)
	}

	// Публикацию задач надо делать атомарно, иначе может быть нарушена консистентность счетчиков
	if err = tx.Conn().Write(func(w db.Writer) (exp error) {
		if exp = tx.Upsert(pairs, mvcc.Writer(w)); exp != nil {
			return
		}

		// Особая магия - инкремент счетчика очереди, чтобы затриггерить подписчиков
		w.Increment(mvcc.WrapKey(q.wrapFlagKey(qFlag, qTriggerKey)), 1)
		return nil
	}); err != nil {
		return ErrPub.WithReason(err)
	}

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

	var pairs []fdb.KeyValue
	var waiter db.Waiter
	var refresh time.Duration

	from := q.wrapFlagKey(qList, nil)

	hdlr := func() error {
		// Критически важно делать это в одной физической транзакции
		// Иначе остается шанс, что одну и ту же задачу возьмут в обработку два воркера
		return cn.Write(func(w db.Writer) (exp error) {
			tx := mvcc.Begin(cn)
			defer tx.Cancel(mvcc.Writer(w))

			if pairs, exp = tx.ListAll(
				ctx,
				mvcc.Last(q.wrapItemKey(time.Now(), nil)),
				mvcc.From(from),
				mvcc.Limit(pack),
				mvcc.SelectPack(1000), // Задачи редко бывают большими
				mvcc.Exclusive(q.onTaskWork),
				mvcc.Writer(w),
			); exp != nil {
				return
			}

			if len(pairs) == 0 {
				// В этом случае не коммитим, т.к. по сути ничего не изменилось
				waiter = w.Watch(mvcc.WrapKey(q.wrapFlagKey(qFlag, qTriggerKey)))

				// Поскольку выборка задач идет в эксклюзивной блокировке, можем
				// этим воспользоваться тут и выбрать время следующей задачи по плану.
				var next []fdb.KeyValue
				if next, exp = tx.ListAll(
					ctx,
					mvcc.Last(from),
					mvcc.From(from),
					mvcc.Limit(1),
					mvcc.Writer(w),
					mvcc.SelectPack(100), // Задачи редко бывают большими
				); exp != nil {
					return
				}

				if len(next) > 0 {
					var when time.Time

					// Применяем обратное экранирование транзакции и очереди, чтобы получить исходный ключ
					// Айдишку мы не знаем, но знаем, что сначала идут флаги коллекции, а затем 8 байт времени
					wkey := fdbx.SkipLeft(next[0].Key, 5+1+len(q.options.prefix))

					if when, exp = fdbx.Byte2Time(wkey[:8]); exp != nil {
						return
					}

					refresh = when.Sub(time.Now())
				} else {
					// Если следующей задачи нет (очередь пуста), ставим таймаут из опций
					refresh = q.options.refresh
				}

				// Если по какой-то причине задача уже в прошлом, большой таймаут не нужен
				if refresh <= 0 {
					refresh = time.Second
				}

				return nil
			}

			// Логический коммит в той же физической транзакции
			// Это самый важный момент - именно благодаря этому перемещенные в процессе чтения
			// элементы очереди будут видны как перемещенные для других логических транзакций
			return tx.Commit(mvcc.Writer(w))
		})
	}

	// Достаем данные по каждой задаче, кроме тех, по которым исходный объект уже удален
	load := func(tx mvcc.Tx) (exp error) {
		list, exp = q.loadTasks(tx, pairs, true)
		return
	}

	for {
		if err = ctx.Err(); err != nil {
			return nil, ErrSub.WithReason(err)
		}

		if waiter != nil {
			q.waitTask(ctx, waiter, refresh)
		}

		if err = hdlr(); err != nil {
			return nil, ErrSub.WithReason(err)
		}

		if len(pairs) == 0 {
			continue
		}

		if err = mvcc.WithTx(cn, load); err != nil {
			return nil, ErrSub.WithReason(err)
		}

		return list, nil
	}
}

func (q v1Queue) waitTask(ctx context.Context, waiter db.Waiter, refresh time.Duration) {
	// Даже если waiter установлен, то при отсутствии других публикаций мы тут зависнем навечно.
	// А задачи, время которых настало, будут просрочены. Для этого нужен особый механизм обработки по таймауту.
	wctx, cancel := context.WithTimeout(ctx, refresh)
	defer cancel()

	// Игнорируем ошибку. Вышли так вышли, главное, что не застряли. Очередь должна работать дальше
	//nolint:errcheck
	_ = waiter.Resolve(wctx)

	// Если запущено много обработчиков, все они рванут забирать события одновременно.
	// Чтобы избежать массовых конфликтов транзакций и улучшить распределение задач делаем небольшую
	// случайную задержку, в пределах 5 мс. Немного для человека, значительно для уменьшения конфликтов
	time.Sleep(time.Duration(rand.Intn(5)) * time.Millisecond)
}

func (q v1Queue) Undo(tx mvcc.Tx, key fdb.Key) (exp error) {
	if exp = tx.Conn().Write(func(w db.Writer) (err error) {
		var pair fdb.KeyValue

		// Загружаем задачу, если она есть
		wkey := q.wrapFlagKey(qWork, key)
		mkey := q.wrapFlagKey(qMeta, key)

		// Выборка элемента из коллекции метаданных
		if pair, err = tx.Select(mkey, mvcc.Writer(w), mvcc.Lock()); err != nil {
			// Задачу уже грохнули, ну и ладно
			if errx.Is(err, mvcc.ErrNotFound) {
				return nil
			}
			return
		}

		// Получаем буфер метаданных
		val := pair.Value

		defer func() {
			if rec := recover(); rec != nil {
				if e, ok := rec.(error); ok {
					err = mvcc.ErrUpsert.WithReason(e)
				} else {
					err = mvcc.ErrUpsert.WithDebug(errx.Debug{"panic": rec})
				}
			}
		}()

		// Целиком распаковывать буфер нам нет смысла, меняем только кол-во попыток и статус
		meta := models.GetRootAsTask(val, 0).State(nil)

		// Если задача уже не висит или в ней не указано плановое время, то не можем грохнуть из плана
		if meta.Planned() == 0 || meta.Status() != StatusPublished {
			return nil
		}

		// Удаляем из списка плановых
		plan := time.Unix(0, meta.Planned()).UTC()
		if err = tx.Delete([]fdb.Key{q.wrapItemKey(plan, key)}, mvcc.Writer(w)); err != nil {
			return
		}

		// Меняем статус задачи
		if !meta.MutateStatus(StatusUnconfirmed) {
			return mvcc.ErrUpsert.WithStack()
		}

		// Сохраняем изменения
		if err = tx.Upsert([]fdb.KeyValue{
			{wkey, key}, // Вставка в коллекцию задач "в работе"
			{mkey, val}, // Вставка в коллекцию метаданных измененного буфера
		}, mvcc.Writer(w)); err != nil {
			return
		}

		return nil
	}); exp != nil {
		return ErrUndo.WithReason(exp)
	}

	return nil

}

func (q v1Queue) Stat(tx mvcc.Tx) (wait, work int64, err error) {
	// Подсчет активных
	if wait, err = tx.Count(q.wrapFlagKey(qList, nil)); err != nil {
		return 0, 0, ErrStat.WithReason(err)
	}

	// Подсчет потеряшек
	if work, err = tx.Count(q.wrapFlagKey(qWork, nil)); err != nil {
		return 0, 0, ErrStat.WithReason(err)
	}

	return wait, work, nil
}

func (q v1Queue) Lost(tx mvcc.Tx, pack int) (list []Task, err error) {
	if pack == 0 {
		return nil, nil
	}

	var pairs []fdb.KeyValue
	wkey := q.wrapFlagKey(qWork, nil)

	if pairs, err = tx.ListAll(
		context.Background(),
		mvcc.Last(wkey),
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

func (q v1Queue) Task(tx mvcc.Tx, key fdb.Key) (res Task, err error) {
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

func (q v1Queue) onTaskWork(tx mvcc.Tx, w db.Writer, p fdb.KeyValue) (err error) {
	var pair fdb.KeyValue

	key := p.Key
	ukey := UnwrapQueueKey(q.prefix, key)
	wkey := q.wrapFlagKey(qWork, ukey)
	mkey := q.wrapFlagKey(qMeta, ukey)

	// Удаление по ключу из основной очереди
	// Физическое удаление - опасный хак, благодаря которому очередь не зависит от сборщика мусора
	// Так можно делать только тут, потому что все логические операции идут в рамках физической транзакции
	if err = tx.Delete([]fdb.Key{key}, mvcc.Writer(w), mvcc.Physical()); err != nil {
		return ErrSub.WithReason(err)
	}

	// Выборка элемента из коллекции метаданных
	if pair, err = tx.Select(mkey, mvcc.Writer(w), mvcc.Lock()); err != nil {
		return ErrSub.WithReason(err)
	}

	// Получаем буфер метаданных
	val := pair.Value

	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(error); ok {
				err = ErrSub.WithReason(e)
			} else {
				err = ErrSub.WithDebug(errx.Debug{"panic": rec})
			}
		}
	}()

	// Целиком распаковывать буфер нам нет смысла, меняем только кол-во попыток и статус
	meta := models.GetRootAsTask(val, 0).State(nil)

	if !meta.MutateRepeats(meta.Repeats() + 1) {
		return ErrSub.WithStack()
	}

	if !meta.MutateStatus(StatusUnconfirmed) {
		return ErrSub.WithStack()
	}

	if err = tx.Upsert([]fdb.KeyValue{
		{wkey, p.Value}, // Вставка в коллекцию задач "в работе"
		{mkey, val},     // Вставка в коллекцию метаданных измененного буфера
	}, mvcc.Writer(w)); err != nil {
		return ErrSub.WithReason(err)
	}

	return nil
}

func (q v1Queue) loadTask(tx mvcc.Tx, key fdb.Key) (tsk *v1Task, err error) {
	var sel fdb.KeyValue

	// Выборка элемента из коллекции метаданных, если его нет - это ужасная ошибка
	if sel, err = tx.Select(q.wrapFlagKey(qMeta, key)); err != nil {
		return
	}

	// Получаем буфер метаданных
	buf := sel.Value

	if len(buf) == 0 {
		return nil, ErrTask.WithStack()
	}

	// Распаковываем модель метаданных задачи
	tsk = &v1Task{q: q, m: models.GetRootAsTask(buf, 0).UnPack()}

	// Так мы достаем объект коллекции. Потенциально удаленный
	if sel, err = q.tb.Select(tx).PossibleByID(key).First(); err == nil {
		tsk.b = sel.Value
	}

	return tsk, nil
}

func (q v1Queue) loadTasks(tx mvcc.Tx, items []fdb.KeyValue, strict bool) (res []Task, err error) {
	var tsk *v1Task

	res = make([]Task, 0, len(items))

	for i := range items {
		// Значение элемента - идентификатор объекта в коллекции
		// Получаем исходные данные объекта как данные задачи
		if tsk, err = q.loadTask(tx, items[i].Value); err != nil {
			if !strict && errx.Is(err, mvcc.ErrNotFound) {
				continue
			}
			return
		}

		// Проверка на то, жив ли объект с исходными данными задачи
		if tsk.b == nil && strict {
			// Если объекта задачи больше нет, то и обработать его нельзя. Удаляем из очереди
			if err = tsk.Ack(tx); err != nil {
				return
			}
			continue
		}

		res = append(res, tsk)
	}

	return res, nil
}

func (q v1Queue) newTask(key fdb.Key, planned time.Time, opts *options) *v1Task {
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
		Key: key,
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

func (q v1Queue) confTask(key fdb.Key) *v1Task {
	return &v1Task{
		q: q,
		m: &models.TaskT{
			Key: key,
			State: &models.TaskStateT{
				Status: StatusConfirmed,
			},
		},
	}
}

type v1Task struct {
	b []byte
	q v1Queue
	m *models.TaskT
}

func (t v1Task) Key() fdb.Key { return t.m.Key }

func (t v1Task) Body() []byte { return t.b }

func (t v1Task) Pair() fdb.KeyValue { return fdb.KeyValue{Key: t.Key(), Value: t.b} }

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

func (t v1Task) Ack(tx mvcc.Tx) error { return t.q.Ack(tx, t.Key()) }

func (t v1Task) Repeat(tx mvcc.Tx, d time.Duration) (err error) {
	if err = t.Ack(tx); err != nil {
		return
	}

	return t.q.Pub(tx, t.Key(), metatask(t.m), Delay(d))
}
