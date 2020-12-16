package mvcc

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/models"
)

/*
	newTx64 - создание и старт новой транзакции MVCC поверх транзакций FDB.

	В первой транзакции мы создаем новый объект, после чего записываем его в новый случайный ключ.
	В момент записи FDB автоматически добавит в значение версию, которую и будем использовать для
	дальнейшей идентификации и кеширования статусов после фиксации.

	Вторая транзакция нужна, чтобы прочитать это значение, удалить его и записать объект как положено.

	Такая несколько схема кажется извращением, но она призвана одновременно достичь следующих целей:
	* Получить монотонно возрастающий идентификатор транзакции, синхронизированный между всеми инстансами
	* Не создать ни одного конфликта записи/чтения между параллельно стартующими транзакциями
*/
func newTx64(conn db.Connection) (t *tx64, err error) {
	t = &tx64{
		conn:   conn,
		start:  uint64(time.Now().UTC().UnixNano()),
		status: txStatusRunning,
	}

	if t.txid, err = sflake.NextID(); err != nil {
		return
	}

	return t, nil
}

type tx64 struct {
	conn   db.Connection
	mods   uint32
	opid   uint32
	txid   uint64
	start  uint64
	locks  locks
	status byte
	oncomm []CommitHandler
}

func (t *tx64) Conn() db.Connection { return t.conn }

func (t *tx64) OnCommit(hdl CommitHandler) {
	t.oncomm = append(t.oncomm, hdl)
}

func (t *tx64) Commit(args ...Option) (err error) {
	opts := getOpts(args)

	if len(t.oncomm) > 0 {
		// Если передается Writer, значит могут быть модификации
		atomic.AddUint32(&t.mods, 1)

		hdlr := func(w db.Writer) (exp error) {
			for i := range t.oncomm {
				if exp = t.oncomm[i](w); exp != nil {
					return
				}
			}
			return nil
		}

		if opts.writer == nil {
			err = t.conn.Write(hdlr)
		} else {
			err = hdlr(opts.writer)
		}

		if err != nil {
			return ErrClose.WithReason(err)
		}
	}

	return t.close(txStatusCommitted, opts.writer)
}

func (t *tx64) Cancel(args ...Option) (err error) {
	opts := getOpts(args)

	return t.close(txStatusAborted, opts.writer)
}

/*
	Delete - удаление актуальной в данный момент записи, если она существует.

	Чтобы найти актуальную запись, нужно сделать по сути обычный Select.
	Удалить - значит обновить значение в служебных полях и записать в тот же ключ.

	Важно, чтобы выборка и обновление шли строго в одной внутренней FDB транзакции.
*/
func (t *tx64) Delete(keys []fdbx.Key, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	hdlr := func(w db.Writer) (exp error) {
		var row fdbx.Pair

		lc := makeCache()
		lg := make([]fdbx.ListGetter, len(keys))

		for i := range keys {
			ukey := WrapKey(keys[i])
			lg[i] = w.List(ukey, ukey, 0, true, false)
		}

		for i := range lg {
			if row, exp = t.fetchRow(w, lc, opid, lg[i]); exp != nil {
				return
			}

			if exp = t.dropRow(w, opid, row, opts.onDelete); exp != nil {
				return
			}
		}

		return nil
	}

	if opts.writer != nil {
		err = hdlr(opts.writer)
	} else {
		err = t.conn.Write(hdlr)
	}

	if err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

/*
	Upsert - вставка или обновление актуальной в данный момент записи.

	Чтобы найти актуальную запись, нужно сделать по сути обычный Select.
	Если актуальной записи не существует, можно просто добавить новую.
	Обновить - значит удалить актуальное значение и добавить новое.

	Важно, чтобы выборка и обновление шли строго в одной внутренней FDB транзакции.
*/
func (t *tx64) Upsert(pairs []fdbx.Pair, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	hdlr := func(w db.Writer) (exp error) {
		var row fdbx.Pair

		lc := makeCache()
		lg := make([]fdbx.ListGetter, len(pairs))

		for i := range pairs {
			ukey := WrapKey(pairs[i].Key())
			lg[i] = w.List(ukey, ukey, 0, true, false)
		}

		for i := range pairs {
			if row, exp = t.fetchRow(w, lc, opid, lg[i]); exp != nil {
				return
			}

			if opts.onInsert != nil {
				if exp = opts.onInsert(t, &usrPair{orig: row}); exp != nil {
					return
				}
			}

			if exp = t.dropRow(w, opid, row, opts.onDelete); exp != nil {
				return
			}

			w.Upsert(&sysPair{
				opid: opid,
				txid: t.txid,
				orig: pairs[i],
			})

			if opts.onUpdate != nil {
				if exp = opts.onUpdate(t, pairs[i].Unwrap()); exp != nil {
					return
				}
			}
		}

		return nil
	}

	if opts.writer != nil {
		err = hdlr(opts.writer)
	} else {
		err = t.conn.Write(hdlr)
	}

	if err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

/*
	Select - выборка актуального в данной транзакции значения ключа.
*/
func (t *tx64) Select(key fdbx.Key, args ...Option) (res fdbx.Pair, err error) {
	ukey := WrapKey(key)
	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	hdlr := func(r db.Reader) (exp error) {
		var ok bool
		var w db.Writer
		var row fdbx.Pair

		if opts.lock {
			// Блокировка не означает модификаций, поэтому mods не увеличиваем
			if w, ok = r.(db.Writer); ok {
				w.Lock(ukey, ukey)
			}
		}

		if row, exp = t.fetchRow(r, nil, opid, r.List(ukey, ukey, 0, true, false)); exp != nil {
			return
		}

		if row == nil {
			return ErrNotFound.WithDebug(errx.Debug{
				"key": key.Printable(),
			})
		}

		res = &usrPair{
			orig: row,
		}

		if opts.onLock != nil {
			// Раз какой-то обработчик в записи, значит модификация - увеличиваем счетчик
			atomic.AddUint32(&t.mods, 1)
			return opts.onLock(t, res, w)
		}

		return nil
	}

	if opts.lock {
		if opts.writer == nil {
			err = t.conn.Write(func(w db.Writer) error { return hdlr(w) })
		} else {
			err = hdlr(opts.writer)
		}
	} else {
		err = t.conn.Read(hdlr)
	}

	if err != nil {
		return nil, ErrSelect.WithReason(err)
	}

	return res, nil
}

func (t *tx64) ListAll(args ...Option) (_ []fdbx.Pair, err error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	list := make([]fdbx.Pair, 0, 128)
	parts, errc := t.seqScan(ctx, args...)

	for part := range parts {
		list = append(list, part...)
	}

	for err = range errc {
		if err != nil {
			return
		}
	}

	return list, nil
}

func (t *tx64) SeqScan(ctx context.Context, args ...Option) (<-chan fdbx.Pair, <-chan error) {
	list := make(chan fdbx.Pair)
	errs := make(chan error, 1)

	go func() {
		defer close(list)
		defer close(errs)

		parts, errc := t.seqScan(ctx, args...)

		for part := range parts {
			for i := range part {
				select {
				case list <- part[i]:
				case <-ctx.Done():
					return
				}
			}
		}

		for err := range errc {
			if err != nil {
				errs <- err
				return
			}
		}
	}()

	return list, errs
}

func (t *tx64) seqScan(ctx context.Context, args ...Option) (<-chan []fdbx.Pair, <-chan error) {
	list := make(chan []fdbx.Pair)
	errs := make(chan error, 1)

	go func() {
		var err error
		var part []fdbx.Pair

		defer close(list)
		defer close(errs)

		size := 0
		rows := 0
		skip := false
		opts := getOpts(args)
		from := WrapKey(opts.from)
		last := WrapKey(opts.last)
		opid := atomic.AddUint32(&t.opid, 1)
		hdlr := func(r db.Reader) (exp error) {
			if opts.reverse {
				rows, part, last, exp = t.selectPart(r, from, last, size, skip, opid, &opts)
			} else {
				rows, part, from, exp = t.selectPart(r, from, last, size, skip, opid, &opts)
			}
			return exp
		}

		for {
			if ctx.Err() != nil {
				return
			}

			if opts.lock {
				if opts.writer == nil {
					err = t.conn.Write(func(w db.Writer) error { return hdlr(w) })
				} else {
					err = hdlr(opts.writer)
				}
			} else {
				err = t.conn.Read(hdlr)
			}

			if err != nil {
				errs <- ErrSeqScan.WithReason(err)
				return
			}

			if len(part) == 0 {
				if rows < MaxRowCount {
					return
				} else {
					continue
				}
			}

			skip = true
			select {
			case list <- part:
				if size += len(part); opts.limit > 0 && size >= opts.limit {
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	return list, errs
}

func (t *tx64) selectPart(
	r db.Reader,
	from, to fdbx.Key,
	size int,
	skip bool,
	opid uint32,
	opts *options,
) (rows int, part []fdbx.Pair, last fdbx.Key, err error) {
	var ok bool
	var w db.Writer

	lg := r.List(from, to, uint64(MaxRowCount), opts.reverse, skip)

	if rows, part, err = t.fetchAll(r, nil, opid, lg); err != nil {
		return
	}

	if len(part) == 0 {
		if opts.reverse {
			return rows, nil, from, nil
		} else {
			return rows, nil, to, nil
		}
	}

	if opts.lock {
		// Блокировка не означает модификаций, поэтому mods не увеличиваем
		if w, ok = r.(db.Writer); ok {
			w.Lock(from, to)
		}
	}

	cnt := len(part)

	if opts.limit > 0 && (size+cnt) > opts.limit {
		cnt = opts.limit - size
	}

	part = part[:cnt]
	last = part[cnt-1].Key()

	for i := range part {
		part[i] = &usrPair{orig: part[i]}

		if opts.onLock != nil {
			// Раз какой-то обработчик в записи, значит модификация - увеличиваем счетчик
			atomic.AddUint32(&t.mods, 1)

			if err = opts.onLock(t, part[i], w); err != nil {
				return
			}
		}
	}

	return rows, part, last, nil
}

func (t *tx64) SaveBLOB(key fdbx.Key, blob []byte, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	sum := 0
	tmp := blob
	num := uint16(0)
	prs := make([]fdbx.Pair, 0, MaxRowMem/MaxRowSize+1)
	set := func(w db.Writer) error { w.Upsert(prs...); return nil }

	// Разбиваем на элементарные значения, пишем пачками по 10 мб
	for {
		if len(tmp) > MaxRowSize {
			prs = append(prs, fdbx.NewPair(WrapKey(key).RPart(byte(num>>8), byte(num)), tmp[:MaxRowSize]))
			tmp = tmp[MaxRowSize:]
			sum += MaxRowSize
			num++
		} else {
			prs = append(prs, fdbx.NewPair(WrapKey(key).RPart(byte(num>>8), byte(num)), tmp))
			sum += len(tmp)
			num++
			break
		}

		if sum >= MaxRowMem-MaxRowSize {
			if err = t.conn.Write(set); err != nil {
				return ErrBLOBSave.WithReason(err)
			}
			sum = 0
			prs = prs[:0]
		}
	}

	if len(prs) > 0 {
		if err = t.conn.Write(set); err != nil {
			return ErrBLOBSave.WithReason(err)
		}
	}

	return nil
}

func (t *tx64) LoadBLOB(key fdbx.Key, size int, args ...Option) (_ []byte, err error) {
	const pack = 100

	var rows []fdbx.Pair

	skip := false
	ukey := WrapKey(key)
	end := ukey.RPart(0xFF, 0xFF)
	res := make([][]byte, 0, pack)
	fnc := func(r db.Reader) (exp error) {
		rows = r.List(ukey, end, pack, false, skip).Resolve()
		return nil
	}

	for {
		if err = t.conn.Read(fnc); err != nil {
			return nil, ErrBLOBLoad.WithReason(err)
		}

		if len(rows) == 0 {
			break
		}

		for i := range rows {
			res = append(res, rows[i].Value())
		}

		ukey = rows[len(rows)-1].Key()
		skip = true
	}

	return bytes.Join(res, nil), nil
}

func (t *tx64) DropBLOB(key fdbx.Key, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	ukey := WrapKey(key)
	hdlr := func(w db.Writer) error { w.Erase(ukey, ukey); return nil }

	if opts.writer == nil {
		err = t.conn.Write(hdlr)
	} else {
		err = hdlr(opts.writer)
	}

	if err != nil {
		return ErrBLOBDrop.WithReason(err)
	}

	return nil
}

func (t *tx64) SharedLock(key fdbx.Key, wait time.Duration) (err error) {
	var lock fdbx.Waiter

	ukey := WrapLockKey(key)
	pair := fdbx.NewPair(ukey, fdbx.Time2Byte(time.Now()))
	hdlr := func(w db.Writer) (exp error) {
		// Если есть значение ключа, значит блокировка занята, придется ждать
		if len(w.Data(ukey).Value()) > 0 {
			lock = w.Watch(ukey)
			return nil
		}

		// Если нам все-таки удается поставить значение - значит блокировка наша
		w.Upsert(pair)
		lock = nil
		return nil
	}

	// Если мы в этой транзакции уже взяли такую блокировку, то нечего и ждать, все ок
	if t.locks.Append(ukey) {
		return nil
	}

	// Ставим контекст с таймаутом. Если он прервется - значит схватили дедлок
	wctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	// Стараемся получить блокировку, если занято - ожидаем
	for {
		// Словили дедлок, выходим
		if err = wctx.Err(); err != nil {
			return ErrSharedLock.WithReason(ErrDeadlock.WithReason(err))
		}

		// Попытка поставить блокировку
		if err = t.conn.Write(hdlr); err != nil {
			return ErrSharedLock.WithReason(err)
		}

		// Блокировка наша, можно ехать дальше
		if lock == nil {
			return nil
		}

		// Значение уже стоит, ждем освобождения, если ошибка - то это скорее всего дедлок
		if err = lock.Resolve(wctx); err != nil {
			return ErrSharedLock.WithReason(ErrDeadlock.WithReason(err))
		}
	}
}

func (t *tx64) isCommitted(local *txCache, r db.Reader, x uint64) (_ bool, err error) {
	var status byte

	if status, err = t.txStatus(local, r, x); err != nil {
		return
	}

	return status == txStatusCommitted, nil
}

func (t *tx64) isAborted(local *txCache, r db.Reader, x uint64) (_ bool, err error) {
	var status byte

	if status, err = t.txStatus(local, r, x); err != nil {
		return
	}

	return status == txStatusAborted, nil
}

func (t *tx64) txStatus(local *txCache, r db.Reader, x uint64) (status byte, err error) {
	// Дешевле всего чекнуть в глобальном кеше, вдруг уже знаем такую
	if status = globCache.get(x); status != txStatusUnknown {
		return
	}

	// Возможно, это открытая транзакция из локального кеша
	if status = local.get(x); status != txStatusUnknown {
		return
	}

	// Придется слазать в БД за статусом и положить в кеш
	val := r.Data(txKey(x)).Value()

	// Если в БД нет записи, то либо транзакция еще открыта, либо это был откат
	// Поскольку нет определенности, в глобальный кеш ничего не складываем
	if len(val) == 0 {
		local.set(x, txStatusRunning)
		return txStatusRunning, nil
	}

	// Получаем значение статуса как часть модели
	status = models.GetRootAsTransaction(val, 0).Status()

	// В случае финальных статусов можем положить в глобальный кеш
	if status == txStatusCommitted || status == txStatusAborted {
		globCache.set(x, status)
	}

	// В локальный кеш можем положить в любом случае, затем вернуть
	local.set(x, status)
	return status, nil
}

func (t *tx64) pack() []byte {
	return fdbx.FlatPack(&models.TransactionT{
		TxID:   t.txid,
		Start:  t.start,
		Status: t.status,
	})
}

/*
	close - закрывает транзакцию и применяет или откатывает изменения.

	Интерпретация транзакций в БД идет следующим образом:

	* Если запись есть в БД и статус "закоммичено" - значит закоммичено
	* Если запись есть в БД и статус "в процессе" - значит еще в процессе
	* Если запись есть в БД и статус "отменено" - значит отменено
	* Если записи нет в БД - то транзакция считается отмененной (aborted)
*/
func (t *tx64) close(status byte, w db.Writer) (err error) {
	// Если статус транзакции уже определен, менять его нельзя
	if t.status == txStatusCommitted || t.status == txStatusAborted {
		return nil
	}

	if err = t.locks.Release(t, w); err != nil {
		return ErrClose.WithReason(err)
	}

	// Получаем ключ транзакции и устанавливаем статус
	txid := txKey(t.txid)
	mods := atomic.LoadUint32(&t.mods)
	t.status = status

	// Если это откат - то сохраняем данные только в случае, если
	// в рамках транзакции могли быть какие-то изменения (счетчик mods > 0)
	// В остальных случаях можем спокойно установить локальный кеш и выйти
	if mods == 0 {
		globCache.set(t.txid, t.status)
		return nil
	}

	// Cохраняем в БД объект с обновленным статусом
	pair := fdbx.NewPair(txid, t.pack())
	hdlr := func(w db.Writer) error { w.Upsert(pair); return nil }

	// Выполняем обработчик в физической транзакции - указанной или новой
	if w == nil {
		err = t.conn.Write(hdlr)
	} else {
		err = hdlr(w)
	}

	if err != nil {
		return ErrClose.WithReason(err)
	}

	// При удачном стечении обстоятельств - устанавливаем глобальный кеш
	globCache.set(t.txid, t.status)
	return nil
}

/*
	fetchRow - выборка одного актуального в данной транзакции значения ключа.

	Каждый ключ для значения на самом деле хранится в нескольких версиях.
	Их можно разделить на три группы: устаревшие, актуальные, незакоммиченные.
	В каждый конкретный момент времени актуальной должна быть только 1 версия.
	Устаревших может быть очень много, это зависит от того, насколько часто объект
	меняется и как хорошо справляется сборщик мусора в БД (автовакуум). В обязанности
	сборщика входит очистка как удаленных версий, так и версий отклоненных транзакций.
	Кол-во незакоммиченных версий зависит от того, сколько транзакций открыто параллельно
	в данный момент.

	Мы должны выбрать все записанные в БД значения для данного ключа, чтобы проверить
	каждое и отклонить все неподходящие. Поскольку это происходит каждый раз, когда нам
	требуется какое-то значение, очевидно, что эффективность работы будет зависеть как
	от кол-ва параллельных запросов (нагрузки), так и от утилизации сборщика мусора.

	Поскольку мы храним список значений в порядке версий их создания, то мы можем
	перебирать значения-кандидаты от самых последних к самым старым. В этом случае
	степень влияния текущей нагрузки будет гораздо выше, чем эффективности сборщика.
	Это выгоднее, потому что если сборщик не справляется, то равномерно по всем ключам.
	А очень частые обновления при нагрузке идут только по некоторым из них, к тому же
	в реальных проектах процент таких ключей не должен быть высок, а нагрузка пиковая.

	Каждая запись содержит 4 служебных поля, определяющих её актуальность, попарно
	отвечающих за глобальную (между транзакциями) и локальную (в рамках транзакции)
	актуальность значений.

	Поскольку мы выбираем весь диапазон в рамках внутренней транзакции fdb, то при параллельных
	действиях в рамках этого диапазона (запись или изменение другой версии этого же значения)
	сработает внутренний механизм конфликтов fdb, так что одновременно двух актуальных
	версий не должно появиться. Один из обработчиков увидит изменения и изменит поведение.
*/
func (t *tx64) fetchRow(r db.Reader, lc *txCache, opid uint32, lg fdbx.ListGetter) (_ fdbx.Pair, err error) {
	var comm bool
	var buf models.RowState
	var row models.RowStateT

	if lc == nil {
		lc = makeCache()
	}

	// Загружаем список версий, существующих в рамках этой операции
	list := lg.Resolve()

	// Проверяем все версии, пока не получим актуальную
	for i := range list {
		val := list[i].Value()

		if len(val) == 0 {
			continue
		}

		models.GetRootAsRow(val, 0).State(&buf).UnPackTo(&row)

		// Частный случай - если запись создана в рамках текущей транзакции
		if row.XMin == t.txid {

			// Если запись создана позже в рамках данной транзакции, то еще не видна
			if row.CMin >= opid {
				continue
			}

			// Поскольку данная транзакция еще не закоммичена, то версия может быть
			// Или удалена в этой же транзакции, или вообще не удалена (или 0, или t.txid)
			// Если запись удалена в текущей транзакции и до этого момента, то уже не видна
			if row.XMax == t.txid && row.CMax < opid {
				continue
			}

			// В этом случае объект создан в данной транзакции и еще не удален, можем прочитать
			return list[i], nil
		}

		if comm, err = t.isCommitted(lc, r, row.XMin); err != nil {
			return
		}

		// Если запись создана другой транзакцией и она еще не закоммичена, то еще не видна
		if !comm {
			continue
		}

		// Проверяем, возможно запись уже была удалена
		switch row.XMax {
		case t.txid:
			// Если она была удалена до этого момента, то уже не видна
			if row.CMax < opid {
				continue
			}

			// В этом случае объект создан в закоммиченной транзакции и удален позже, чем мы тут читаем
			fallthrough
		case 0:
			// В этом случае объект создан в закоммиченной транзакции и еще не был удален
			return list[i], nil
		default:
			// Если запись была удалена другой транзакцией, но она еще закоммичена, то еще видна
			if comm, err = t.isCommitted(lc, r, row.XMax); err != nil {
				return
			}

			if !comm {
				return list[i], nil
			}
		}
	}

	return nil, nil
}

// Аналогично fetchRow, но все актуальные версии всех ключей по диапазону
func (t *tx64) fetchAll(r db.Reader, lc *txCache, opid uint32, lg fdbx.ListGetter) (cnt int, res []fdbx.Pair, err error) {
	var comm bool
	var buf models.RowState
	var row models.RowStateT

	if lc == nil {
		lc = makeCache()
	}

	list := lg.Resolve()
	res = list[:0]
	cnt = len(list)

	// Проверяем все версии, пока не получим актуальную
	for i := range list {
		val := list[i].Value()

		if len(val) == 0 {
			continue
		}

		models.GetRootAsRow(val, 0).State(&buf).UnPackTo(&row)

		// Частный случай - если запись создана в рамках текущей транзакции
		if row.XMin == t.txid {

			// Если запись создана позже в рамках данной транзакции, то еще не видна
			if row.CMin >= opid {
				continue
			}

			// Поскольку данная транзакция еще не закоммичена, то версия может быть
			// Или удалена в этой же транзакции, или вообще не удалена (или 0, или t.txid)
			// Если запись удалена в текущей транзакции и до этого момента, то уже не видна
			if row.XMax == t.txid && row.CMax < opid {
				continue
			}

			// В этом случае объект создан в данной транзакции и еще не удален, можем прочитать
			res = append(res, list[i])
			continue
		}

		if comm, err = t.isCommitted(lc, r, row.XMin); err != nil {
			return
		}

		// Если запись создана другой транзакцией и она еще не закоммичена, то еще не видна
		if !comm {
			continue
		}

		// Проверяем, возможно запись уже была удалена
		switch row.XMax {
		case t.txid:
			// Если она была удалена до этого момента, то уже не видна
			if row.CMax < opid {
				continue
			}

			// В этом случае объект создан в закоммиченной транзакции и удален позже, чем мы тут читаем
			fallthrough
		case 0:
			// В этом случае объект создан в закоммиченной транзакции и еще не был удален
			res = append(res, list[i])
			continue
		default:
			// Если запись была удалена другой транзакцией, но она еще закоммичена, то еще видна
			if comm, err = t.isCommitted(lc, r, row.XMax); err != nil {
				return
			}
			if !comm {
				res = append(res, list[i])
				continue
			}
		}
	}

	// Стираем хвост, чтобы сборщик мусора пришел за ним
	for i := len(res); i < len(list); i++ {
		list[i] = nil
	}

	return cnt, res, nil
}

func (t *tx64) dropRow(w db.Writer, opid uint32, pair fdbx.Pair, onDelete Handler) (err error) {
	if pair == nil {
		return nil
	}

	atomic.AddUint32(&t.mods, 1)

	buf := pair.Value()

	if len(buf) > 0 {
		row := models.GetRootAsRow(buf, 0)

		// Обработчик имеет возможность предотвратить удаление/обновление, выбросив ошибку
		if onDelete != nil {
			if err = onDelete(t, fdbx.NewPair(UnwrapKey(pair.Key()), row.DataBytes())); err != nil {
				return ErrDelete.WithReason(err)
			}
		}

		// В модели состояния поля указаны как required
		// Обновление должно произойти 100%, если только это не косяк модели или баг библиотеки
		if state := row.State(nil); !(state.MutateXMax(t.txid) && state.MutateCMax(opid)) {
			return ErrDelete.WithStack()
		}
	} else {
		// По идее сюда никогда не должны попасть, но на всякий случай - элемент автовосстановления стабильности
		buf = fdbx.FlatPack(&models.RowT{
			State: &models.RowStateT{
				XMin: t.txid,
				XMax: t.txid,
				CMin: 0,
				CMax: opid,
			},
			Data: nil,
		})
	}

	// Обновляем данные в БД по ключу
	w.Upsert(fdbx.NewPair(pair.Key(), buf))
	return nil
}

func (t *tx64) Touch(key fdbx.Key) {
	t.OnCommit(func(w db.Writer) error {
		w.Upsert(fdbx.NewPair(WrapWatchKey(key), fdbx.Time2Byte(time.Now())))
		return nil
	})
}

func (t *tx64) Watch(key fdbx.Key) (wait fdbx.Waiter, err error) {
	return wait, t.conn.Write(func(w db.Writer) error {
		wait = w.Watch(WrapWatchKey(key))
		return nil
	})
}

func (t *tx64) Vacuum(prefix fdbx.Key, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	skip := false
	opts := getOpts(args)
	from := WrapKey(prefix)
	to := from.Clone()

	for {
		if err = t.conn.Write(func(w db.Writer) (exp error) {
			lg := w.List(from, to, uint64(opts.packSize), false, skip)

			if from, exp = t.vacuumPart(w, lg, opts.onVacuum); exp != nil {
				return
			}

			return nil
		}); err != nil {
			return ErrVacuum.WithReason(err)
		}

		// Пустой ключ - значит больше не было строк, условие выхода
		if from == nil {
			return nil
		}
		skip = true
	}
}

// vacuumPart - функция обратная fetchRow, в том смысле, что она удаляет все ключи, которые больше не нужны в БД
func (t *tx64) vacuumPart(w db.Writer, lg fdbx.ListGetter, onVacuum RowHandler) (_ fdbx.Key, err error) {
	var ok bool
	var val []byte
	var buf models.RowState
	var row models.RowStateT

	list := lg.Resolve()

	// Больше нечего получить - условие выхода
	if len(list) == 0 {
		return nil, nil
	}

	lc := makeCache()
	drop := make([]fdbx.Pair, 0, len(list))

	atomic.AddUint32(&t.mods, 1)

	// Проверяем все пары ключ/значение, удаляя все, что может помешать
	for i := range list {
		val = list[i].Value()

		// Могут попадаться не только значения mvcc, но и произвольные счетчики
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					val = nil
				}
			}()
			models.GetRootAsRow(val, 0).State(&buf).UnPackTo(&row)
		}()

		if len(val) == 0 {
			continue
		}

		// Частный случай - если запись создана в рамках текущей транзакции
		if row.XMin == t.txid {
			// Хотя в ряде случаев запись уже можно удалять, но для упрощения логики не будем.
			// К тому же, это будет транзакция очистки, вряд ли мы вообще сюда когда-то попадем
			continue
		}

		// Проверяем статус транзакции, которая создала запись
		if ok, err = t.isAborted(lc, w, row.XMin); err != nil {
			return
		}

		// Удаляем, если запись точно ушла в мусор в самом начале
		if ok {
			drop = append(drop, list[i])
			continue
		}

		// Если запись еще не удалена, то в любом случае не трогаем
		// Если она удалена данной транзакцией, то для упрощения тоже пока не трогаем
		if row.XMax == 0 || row.XMax == t.txid {
			continue
		}

		// Проверяем статус транзакции, которая создала запись
		if ok, err = t.isCommitted(lc, w, row.XMax); err != nil {
			return
		}

		// Если запись была удалена другой транзакцией, и она закоммичена, то смело можем удалять
		if ok {
			drop = append(drop, list[i])
		}
	}

	// Теперь можем грохнуть все, что нашли
	for i := range drop {
		if onVacuum != nil {
			if err = onVacuum(t, &usrPair{orig: drop[i]}, w); err != nil {
				return
			}
		}

		w.Delete(drop[i].Key())
	}

	// Возвращаем последний проверенный ключ, с которого надо начать след. цикл
	return list[len(list)-1].Key(), nil
}

type locks struct {
	sync.Mutex
	acks map[string]fdbx.Key
}

func (l *locks) Append(key fdbx.Key) (exists bool) {
	l.Lock()
	defer l.Unlock()

	if l.acks == nil {
		l.acks = make(map[string]fdbx.Key, 1)
	}

	if l.acks[key.Printable()] != nil {
		return true
	}

	l.acks[key.Printable()] = key
	return false
}

func (l *locks) Release(t *tx64, w db.Writer) (err error) {
	l.Lock()
	defer l.Unlock()

	if len(l.acks) == 0 {
		return nil
	}

	// Если передается Writer, значит могут быть модификации
	atomic.AddUint32(&t.mods, 1)

	hdlr := func(w db.Writer) (exp error) {
		for _, k := range l.acks {
			w.Delete(k)
		}
		return nil
	}

	if w == nil {
		return t.conn.Write(hdlr)
	}

	return hdlr(w)
}
