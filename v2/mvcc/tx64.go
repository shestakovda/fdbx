package mvcc

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
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
func newTx64(conn db.Connection) *tx64 {
	return &tx64{
		conn:   conn,
		txid:   newTxID(),
		start:  time.Now().UTC().UnixNano(),
		status: txStatusRunning,
	}
}

type tx64 struct {
	conn   db.Connection
	mods   uint32
	opid   uint32
	txid   []byte
	start  int64
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
		var rows []fdbx.Pair

		lc := makeCache()
		kl := make([]int, len(keys))
		lg := make([]fdbx.ListGetter, len(keys))

		for i := range keys {
			ukey := WrapKey(keys[i])
			kl[i] = len(ukey.Bytes())
			lg[i] = w.List(ukey, ukey, 0, true, false)
		}

		for i := range lg {
			if rows, exp = t.fetchRows(w, lc, opid, lg[i], true, kl[i]); exp != nil {
				return
			}

			if exp = t.dropRows(w, opid, rows, opts.onDelete, opts.physical); exp != nil {
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
		var rows []fdbx.Pair

		lc := makeCache()
		kl := make([]int, len(pairs))
		lg := make([]fdbx.ListGetter, len(pairs))

		for i := range pairs {
			ukey := WrapKey(pairs[i].Key())
			kl[i] = len(ukey.Bytes())
			lg[i] = w.List(ukey, ukey, 0, true, false)
		}

		for i := range pairs {
			if rows, exp = t.fetchRows(w, lc, opid, lg[i], true, kl[i]); exp != nil {
				return
			}

			if opts.onUpdate != nil && len(rows) > 0 {
				if exp = opts.onUpdate(t, w, pairs[i]); exp != nil {
					return
				}
			}

			if exp = t.dropRows(w, opid, rows, opts.onDelete, false); exp != nil {
				return
			}

			if opts.onInsert != nil {
				if exp = opts.onInsert(t, w, pairs[i]); exp != nil {
					return
				}
			}

			w.Upsert(&sysPair{
				opid: opid,
				txid: t.txid,
				orig: pairs[i],
			})
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
		var rows []fdbx.Pair

		if opts.lock {
			// Блокировка не означает модификаций, поэтому mods не увеличиваем
			if w, ok = r.(db.Writer); ok {
				w.Lock(ukey, ukey)
			}
		}

		lc := makeCache()
		kl := len(ukey.Bytes())
		lg := r.List(ukey, ukey, 0, true, false)

		if rows, exp = t.fetchRows(r, lc, opid, lg, false, kl); exp != nil {
			return
		}

		if len(rows) == 0 {
			return ErrNotFound.WithDebug(errx.Debug{
				"key": key.Printable(),
			})
		}

		if len(rows) > 1 {
			return ErrDuplicate.WithDebug(errx.Debug{
				"key": key.Printable(),
			})
		}

		res = &usrPair{
			orig: rows[0],
		}

		if opts.onLock != nil {
			// Раз какой-то обработчик в записи, значит модификация - увеличиваем счетчик
			atomic.AddUint32(&t.mods, 1)
			return opts.onLock(t, w, res)
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

func (t *tx64) ListAll(ctx context.Context, args ...Option) (_ []fdbx.Pair, err error) {
	ctx, cancel := context.WithCancel(ctx)
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
		lcch := makeCache()
		opts := getOpts(args)
		from := WrapKey(opts.from)
		last := WrapKey(opts.last)
		opid := atomic.AddUint32(&t.opid, 1)
		hdlr := func(r db.Reader) (exp error) {
			if opts.reverse {
				rows, part, last, exp = t.selectPart(r, lcch, from, last, size, skip, opid, &opts)
			} else {
				rows, part, from, exp = t.selectPart(r, lcch, from, last, size, skip, opid, &opts)
			}
			return exp
		}

		if Debug {
			glog.Infof("tx64.seqScan(%s, %s, %d)", from.Printable(), last.Printable(), opts.limit)
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

			if Debug {
				glog.Infof("tx64.seqScan.from = %s", from.Printable())
				glog.Infof("tx64.seqScan.last = %s", last.Printable())
				glog.Infof("tx64.seqScan.rows = %d", rows)
				glog.Infof("tx64.seqScan.part = %d", len(part))
			}

			if len(part) == 0 {
				if rows < int(opts.spack) {
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
	lc *txCache,
	from, to fdbx.Key,
	size int,
	skip bool,
	opid uint32,
	opts *options,
) (rows int, part []fdbx.Pair, last fdbx.Key, err error) {
	var ok bool
	var w db.Writer

	if Debug {
		glog.Infof("tx64.selectPart(%s, %s, %d, %d)", from.Printable(), to.Printable(), size, opts.spack)
	}

	lg := r.List(from, to, opts.spack, opts.reverse, skip)

	if part, err = t.fetchRows(r, lc, opid, lg, false, 0); err != nil {
		return
	}

	// вызов из кеша
	list := lg.Resolve()

	if rows = len(list); rows == 0 {
		if opts.reverse {
			return rows, nil, from, nil
		} else {
			return rows, nil, to, nil
		}
	}
	last = list[len(list)-1].Key()

	if Debug {
		glog.Infof("tx64.selectPart.rows = %d", rows)
		glog.Infof("tx64.selectPart.part = %d", len(part))
		glog.Infof("tx64.selectPart.last = %s", last.Printable())
	}

	if len(part) == 0 {
		return rows, nil, last, nil
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

	for i := range part {
		part[i] = &usrPair{orig: part[i]}

		if opts.onLock != nil {
			// Раз какой-то обработчик в записи, значит модификация - увеличиваем счетчик
			atomic.AddUint32(&t.mods, 1)

			if err = opts.onLock(t, w, part[i]); err != nil {
				return
			}
		}
	}

	return rows, part, last, nil
}

func (t *tx64) SaveBLOB(key fdbx.Key, blob []byte, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)
	opts := getOpts(args)

	sum := 0
	tmp := blob
	num := uint16(0)
	prs := make([]fdbx.Pair, 0, opts.rowmem/opts.rowsize+1)
	set := func(w db.Writer) error { w.Upsert(prs...); return nil }

	// Разбиваем на элементарные значения, пишем пачками по 10 мб
	for {
		if len(tmp) > opts.rowsize {
			prs = append(prs, fdbx.NewPair(WrapKey(key).RPart(byte(num>>8), byte(num)), tmp[:opts.rowsize]))
			tmp = tmp[opts.rowsize:]
			sum += opts.rowsize
			num++
		} else {
			prs = append(prs, fdbx.NewPair(WrapKey(key).RPart(byte(num>>8), byte(num)), tmp))
			sum += len(tmp)
			num++
			break
		}

		if sum >= opts.rowmem-opts.rowsize {
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

func (t *tx64) isCommitted(local *txCache, r db.Reader, txid []byte) (_ bool, err error) {
	var status byte

	if status, err = t.txStatus(local, r, txid); err != nil {
		return
	}

	return status == txStatusCommitted, nil
}

func (t *tx64) isAborted(local *txCache, r db.Reader, txid []byte) (_ bool, err error) {
	var status byte

	if status, err = t.txStatus(local, r, txid); err != nil {
		return
	}

	return status == txStatusAborted, nil
}

func (t *tx64) txStatus(local *txCache, r db.Reader, txid []byte) (status byte, err error) {
	uid := string(txid)

	// Дешевле всего чекнуть в глобальном кеше, вдруг уже знаем такую
	if status = globCache.get(uid); status != txStatusUnknown {
		return
	}

	// Возможно, это открытая транзакция из локального кеша
	if status = local.get(uid); status != txStatusUnknown {
		return
	}

	// Придется слазать в БД за статусом и положить в кеш
	val := r.Data(WrapTxKey(fdbx.Bytes2Key(txid))).Value()

	// Если в БД нет записи, то либо транзакция еще открыта, либо это был откат
	// Поскольку нет определенности, в глобальный кеш ничего не складываем
	if len(val) == 0 {
		local.set(uid, txStatusRunning)
		return txStatusRunning, nil
	}

	// Получаем значение статуса как часть модели
	status = models.GetRootAsTransaction(val, 0).Status()

	// В случае финальных статусов можем положить в глобальный кеш
	if status == txStatusCommitted || status == txStatusAborted {
		globCache.set(uid, status)
	}

	// В локальный кеш можем положить в любом случае, затем вернуть
	local.set(uid, status)
	return status, nil
}

func (t *tx64) pack() []byte {
	return fdbx.FlatPack(&models.TransactionT{
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
	uid := string(t.txid)
	mods := atomic.LoadUint32(&t.mods)
	t.status = status

	// Если это откат - то сохраняем данные только в случае, если
	// в рамках транзакции могли быть какие-то изменения (счетчик mods > 0)
	// В остальных случаях можем спокойно установить локальный кеш и выйти
	if mods == 0 {
		globCache.set(uid, t.status)
		return nil
	}

	// Cохраняем в БД объект с обновленным статусом
	pair := fdbx.NewPair(WrapTxKey(fdbx.Bytes2Key(t.txid)), t.pack())
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
	globCache.set(uid, t.status)
	return nil
}

/*
	isVisible - проверка актуального в данной транзакции значения ключа.

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
func (t *tx64) isVisible(r db.Reader, lc *txCache, opid uint32, item fdbx.Pair, dirty bool) (ok bool, err error) {
	var ptr [16]byte
	var comm, fail bool

	// Нужна защита от паники, на случай левых или битых записей
	defer func() {
		if rec := recover(); rec != nil {
			glog.Errorf("panic mvcc.tx64.isVisible(%s): %+v", item.Key().Printable(), rec)
			ok = false
			err = errx.ErrInternal.WithDebug(errx.Debug{
				"key":   item.Key().Printable(),
				"panic": rec,
			})
		}
	}()

	key := item.Key().Bytes()
	copy(ptr[:], key[len(key)-16:len(key)])
	cmin := binary.BigEndian.Uint32(ptr[8:12])
	xmin := append(ptr[:8], ptr[12:16]...)
	row := models.GetRootAsRow(item.Value(), 0).UnPack()

	if Debug {
		defer func() { glog.Infof("isVisible(%s) = %t", item.Key().Printable(), ok) }()
	}

	// Частный случай - если запись создана в рамках текущей транзакции
	// то даже если запись создана позже, тут возвращаем, чтобы можно было выполнить
	// "превентивное" удаление и чтобы не было никаких конфликтующих строк
	// Поэтому проверять будем только сторонние транзакции
	if bytes.Equal(xmin, t.txid) {
		if !dirty {
			// Если запись создана позже в рамках данной транзакции, то еще не видна
			if cmin >= opid {
				return false, nil
			}
		}
	} else if dirty {

		// Проверим, была ли отменена транзакция создания этой записи
		if fail, err = t.isAborted(lc, r, xmin); err != nil {
			return
		}

		// Если запись отменена, то в любом случае она нас больше не инетересует
		if fail {
			return false, nil
		}
	} else {
		// Проверим, была ли завершена транзакция создания этой записи
		if comm, err = t.isCommitted(lc, r, xmin); err != nil {
			return
		}

		// Если запись создана другой транзакцией и она еще не закоммичена, то еще не видна
		if !comm {
			return false, nil
		}
	}

	// В этом случае объект еще не был удален, значит виден
	if len(row.Drop) == 0 {
		return true, nil
	}

	// Эта запись уже была удалена этой или параллельной транзакцией
	// Другая транзакция может удалить эту запись "превентивно", на случай конфликтов
	for i := range row.Drop {
		// Учитываем изменения, если запись удалена в текущей транзакции
		if bytes.Equal(row.Drop[i].Tx, t.txid) {
			// Если удалена до этого момента, то уже не видна
			// Если "позже" в параллельном потоке - то еще можно читать
			return row.Drop[i].Op > opid, nil
		}

		// Проверим, была ли завершена транзакция удаления этой записи
		if comm, err = t.isCommitted(lc, r, row.Drop[i].Tx); err != nil {
			return
		}

		// Мы должны учитывать изменения, сделанные закоммиченными транзакциями
		if comm {
			return false, nil
		}
	}

	// Если не нашли "значимых" для нас удалений - значит запись еще видна
	return true, nil
}

// fetchRows - все актуальные версии всех ключей по диапазону
func (t *tx64) fetchRows(
	r db.Reader,
	lc *txCache,
	opid uint32,
	lg fdbx.ListGetter,
	dirty bool,
	exact int,
) (res []fdbx.Pair, err error) {
	var ok bool

	list := lg.Resolve()
	res = make([]fdbx.Pair, 0, len(list))

	// Проверяем все версии, пока не получим актуальную
	for i := range list {
		if exact > 0 && len(list[i].Key().Bytes()) != (exact+16) {
			continue
		}

		if ok, err = t.isVisible(r, lc, opid, list[i], dirty); err != nil {
			return
		}

		if ok {
			res = append(res, list[i])
		}
	}

	return res, nil
}

func (t *tx64) dropRows(w db.Writer, opid uint32, pairs []fdbx.Pair, onDelete RowHandler, physical bool) (err error) {
	var row *models.RowT

	if len(pairs) == 0 {
		return nil
	}

	atomic.AddUint32(&t.mods, 1)

	for _, pair := range pairs {

		if buf := pair.Value(); len(buf) > 0 {
			row = models.GetRootAsRow(buf, 0).UnPack()

			// Обработчик имеет возможность предотвратить удаление/обновление, выбросив ошибку
			if onDelete != nil {
				if err = onDelete(t, w, fdbx.NewPair(UnwrapKey(pair.Key()), row.Data)); err != nil {
					return ErrDelete.WithReason(err)
				}
			}

			row.Drop = append(row.Drop, &models.TxPtrT{
				Tx: t.txid,
				Op: opid,
			})
		} else {
			// По идее сюда никогда не должны попасть, но на всякий случай - элемент автовосстановления стабильности
			row = &models.RowT{
				Drop: []*models.TxPtrT{{
					Tx: t.txid,
					Op: opid,
				}},
				Data: nil,
			}
		}

		// Обновляем данные в БД по ключу
		if physical {
			w.Delete(pair.Key())
		} else {
			w.Upsert(fdbx.NewPair(pair.Key(), fdbx.FlatPack(row)))
		}
	}
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
			lg := w.List(from, to, opts.vpack, false, skip)

			if from, exp = t.vacuumPart(w, lg, opts.onVacuum); exp != nil {
				return
			}

			return nil
		}); err != nil {
			return ErrVacuum.WithReason(err)
		}

		// Пустой ключ - значит больше не было строк, условие выхода
		if len(from.Bytes()) == 0 {
			return nil
		}
		skip = true

		// Передышка, чтобы не слишком грузить бд
		time.Sleep(10 * time.Millisecond)
	}
}

// vacuumPart - функция обратная fetchRows, в том смысле, что она удаляет все ключи, которые больше не нужны в БД
func (t *tx64) vacuumPart(w db.Writer, lg fdbx.ListGetter, onVacuum RowHandler) (_ fdbx.Key, err error) {
	var ok bool

	list := lg.Resolve()

	// Больше нечего получить - условие выхода
	if len(list) == 0 {
		return fdbx.Bytes2Key(nil), nil
	}

	lc := makeCache()
	drop := make([]fdbx.Pair, 0, len(list))
	opid := atomic.AddUint32(&t.opid, 1)

	atomic.AddUint32(&t.mods, 1)

	// Проверяем все пары ключ/значение, удаляя все, что может помешать
	for i := range list {
		if ok, err = t.isVisible(w, lc, opid, list[i], true); err != nil {
			// Ошибку игнорим, потому что это вакуум, важно не удалить лишнего
			continue
		}

		if !ok {
			drop = append(drop, list[i])
		}
	}

	// Теперь можем грохнуть все, что нашли
	for i := range drop {
		if onVacuum != nil {
			if err = onVacuum(t, w, &usrPair{orig: drop[i]}); err != nil {
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
