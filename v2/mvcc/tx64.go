package mvcc

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

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
	txid   suid
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

		if opts.writer.Empty() {
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
func (t *tx64) Delete(keys []fdb.Key, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	hdlr := func(w db.Writer) (exp error) {
		var rows []fdb.KeyValue

		lc := makeCache()
		kl := make([]int, len(keys))
		lg := make([]fdb.RangeResult, len(keys))

		for i := range keys {
			ukey := WrapKey(keys[i])
			kl[i] = len(ukey)
			lg[i] = w.List(ukey, ukey, 0, true, false)
		}

		for i := range lg {
			if rows, exp = t.fetchRows(w.Reader, lc, opid, lg[i], true, kl[i]); exp != nil {
				return
			}

			if exp = t.dropRows(w, opid, rows, opts.onDelete, opts.physical); exp != nil {
				return
			}
		}

		return nil
	}

	if opts.writer.Empty() {
		err = t.conn.Write(hdlr)
	} else {
		err = hdlr(opts.writer)
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
func (t *tx64) Upsert(pairs []fdb.KeyValue, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	hdlr := func(w db.Writer) (exp error) {
		var rows []fdb.KeyValue

		lc := makeCache()
		kl := make([]int, len(pairs))
		lg := make([]fdb.RangeResult, len(pairs))

		for i := range pairs {
			ukey := WrapKey(pairs[i].Key)
			kl[i] = len(ukey)
			lg[i] = w.List(ukey, ukey, 0, true, false)
		}

		for i := range pairs {
			if rows, exp = t.fetchRows(w.Reader, lc, opid, lg[i], true, kl[i]); exp != nil {
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

			w.Upsert(sysPair(opid, pairs[i].Key, t.txid[:], pairs[i].Value))

			if opts.onInsert != nil {
				if exp = opts.onInsert(t, w, pairs[i]); exp != nil {
					return
				}
			}

		}

		return nil
	}

	if opts.writer.Empty() {
		err = t.conn.Write(hdlr)
	} else {
		err = hdlr(opts.writer)
	}

	if err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

/*
	Select - выборка актуального в данной транзакции значения ключа.
*/
func (t *tx64) Select(key fdb.Key, args ...Option) (res fdb.KeyValue, err error) {
	ukey := WrapKey(key)
	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	read := func(r db.Reader) (exp error) {
		var rows []fdb.KeyValue

		lc := makeCache()
		kl := len(ukey)
		lg := r.List(ukey, ukey, 0, true, false)

		if rows, exp = t.fetchRows(r, lc, opid, lg, false, kl); exp != nil {
			return
		}

		if len(rows) == 0 {
			return ErrNotFound.WithDebug(errx.Debug{"key": key})
		}

		if len(rows) > 1 {
			return ErrDuplicate.WithDebug(errx.Debug{"key": key})
		}

		res = usrPair(rows[0])
		return nil
	}
	hdlr := func(w db.Writer) (exp error) {
		if opts.lock {
			// Блокировка не означает модификаций, поэтому mods не увеличиваем
			w.Lock(ukey, ukey)
		}

		if exp = read(w.Reader); exp != nil {
			return
		}

		if opts.onLock != nil {
			// Раз какой-то обработчик в записи, значит модификация - увеличиваем счетчик
			atomic.AddUint32(&t.mods, 1)
			return opts.onLock(t, w, res)
		}

		return nil
	}

	if opts.lock {
		if opts.writer.Empty() {
			err = t.conn.Write(func(w db.Writer) error { return hdlr(w) })
		} else {
			err = hdlr(opts.writer)
		}
	} else {
		err = t.conn.Read(read)
	}

	if err != nil {
		return fdb.KeyValue{}, ErrSelect.WithReason(err)
	}

	return res, nil
}

func (t *tx64) ListAll(ctx context.Context, args ...Option) (_ []fdb.KeyValue, err error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	list := make([]fdb.KeyValue, 0, 128)
	parts, errc := t.SeqScan(ctx, args...)

	for part := range parts {
		list = append(list, part)
	}

	for err = range errc {
		if err != nil {
			return
		}
	}

	return list, nil
}

type kvHead struct {
	size int
	head *kvPtr
}

type kvPtr struct {
	fdb.KeyValue
	next *kvPtr
}

func (t *tx64) SeqScan(ctx context.Context, args ...Option) (<-chan fdb.KeyValue, <-chan error) {
	list := make(chan fdb.KeyValue)
	errs := make(chan error, 1)

	go func() {
		var err error
		var part kvHead

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
		hdlr := func(w db.Writer) (exp error) {
			if opts.reverse {
				rows, part, last, exp = t.selectPart(ctx, w, lcch, from, last, size, skip, opid, &opts)
			} else {
				rows, part, from, exp = t.selectPart(ctx, w, lcch, from, last, size, skip, opid, &opts)
			}
			return exp
		}

		if Debug {
			glog.Infof("tx64.seqScan(%s, %s, %d)", from, last, opts.limit)
		}

		for {
			if ctx.Err() != nil {
				return
			}

			if opts.writer.Empty() {
				err = t.conn.Write(hdlr)
			} else {
				err = hdlr(opts.writer)
			}

			if err != nil {
				errs <- ErrSeqScan.WithReason(err)
				return
			}

			if Debug {
				glog.Infof("tx64.seqScan.from = %s", from)
				glog.Infof("tx64.seqScan.last = %s", last)
				glog.Infof("tx64.seqScan.rows = %d", rows)
				glog.Infof("tx64.seqScan.part = %d", part)
			}

			skip = true

			if part.size == 0 {
				if rows < int(opts.spack) || (opts.spack == 0 && rows == 0) {
					return
				}
				continue
			}

			item := part.head
			for item != nil {
				select {
				case list <- item.KeyValue:
					if size++; opts.limit > 0 && size >= opts.limit {
						return
					}
				case <-ctx.Done():
					return
				}
				item = item.next
			}
		}
	}()

	return list, errs
}

func (t *tx64) selectPart(
	ctx context.Context,
	w db.Writer,
	lc *txCache,
	from, to fdb.Key,
	size int,
	skip bool,
	opid uint32,
	opts *options,
) (rows int, part kvHead, last fdb.Key, err error) {
	var ok bool

	if Debug {
		glog.Infof("tx64.selectPart(%s, %s, %d, %d)", from, to, size, opts.spack)
	}

	if opts.lock {
		// Блокировка не означает модификаций, поэтому mods не увеличиваем
		w.Lock(from, to)
	}

	lg := w.List(from, to, opts.spack, opts.reverse, skip)

	wctx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()

	var ptr *kvPtr
	iter := lg.Iterator()
	for iter.Advance() {
		item := iter.MustGet()
		item.Key = item.Key[1:]

		if ok, err = t.isVisible(w.Reader, lc, opid, item, false); err != nil {
			return
		}

		last = item.Key
		rows++

		if ok {
			item = usrPair(item)

			if opts.onLock != nil {
				// Раз какой-то обработчик в записи, значит модификация - увеличиваем счетчик
				atomic.AddUint32(&t.mods, 1)

				if err = opts.onLock(t, w, item); err != nil {
					return
				}
			}

			if ptr == nil {
				part.head = &kvPtr{
					KeyValue: item,
				}
				ptr = part.head
			} else {
				ptr.next = &kvPtr{
					KeyValue: item,
				}
				ptr = ptr.next
			}

			if part.size++; opts.limit > 0 && size+part.size >= opts.limit {
				break
			}
		}

		if wctx.Err() != nil {
			break
		}
	}

	if Debug {
		glog.Infof("tx64.selectPart.rows = %d", rows)
		glog.Infof("tx64.selectPart.part = %d", part.size)
		glog.Infof("tx64.selectPart.last = %s", last)
	}

	if rows == 0 {
		if opts.reverse {
			last = from
		} else {
			last = to
		}
	}

	return rows, part, last, nil
}

func (t *tx64) SaveBLOB(key fdb.Key, blob []byte, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)
	opts := getOpts(args)

	sum := 0
	tmp := blob
	num := uint16(0)
	prs := make([]fdb.KeyValue, 0, opts.rowmem/opts.rowsize+1)
	set := func(w db.Writer) error { w.Upsert(prs...); return nil }

	// Разбиваем на элементарные значения, пишем пачками по 10 мб
	for {
		if len(tmp) > opts.rowsize {
			prs = append(prs, fdb.KeyValue{fdbx.AppendRight(WrapKey(key), byte(num>>8), byte(num)), tmp[:opts.rowsize]})
			tmp = tmp[opts.rowsize:]
			sum += opts.rowsize
			num++
		} else {
			prs = append(prs, fdb.KeyValue{fdbx.AppendRight(WrapKey(key), byte(num>>8), byte(num)), tmp})
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

func (t *tx64) LoadBLOB(key fdb.Key, size int, args ...Option) (_ []byte, err error) {
	const pack = 100

	var rows []fdb.KeyValue

	skip := false
	ukey := WrapKey(key)
	end := fdbx.AppendRight(ukey, 0xFF, 0xFF)
	res := make([][]byte, 0, pack)
	fnc := func(r db.Reader) (exp error) {
		rows = r.List(ukey, end, pack, false, skip).GetSliceOrPanic()
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
			res = append(res, rows[i].Value)
		}

		ukey = rows[len(rows)-1].Key[1:]
		skip = true
	}

	return bytes.Join(res, nil), nil
}

func (t *tx64) DropBLOB(key fdb.Key, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	ukey := WrapKey(key)
	hdlr := func(w db.Writer) error { w.Erase(ukey, ukey); return nil }

	if opts.writer.Empty() {
		err = t.conn.Write(hdlr)
	} else {
		err = hdlr(opts.writer)
	}

	if err != nil {
		return ErrBLOBDrop.WithReason(err)
	}

	return nil
}

func (t *tx64) SharedLock(key fdb.Key, wait time.Duration) (err error) {
	var lock fdbx.Waiter

	ukey := WrapLockKey(key)
	pair := fdb.KeyValue{ukey, fdbx.Time2Byte(time.Now())}
	hdlr := func(w db.Writer) (exp error) {
		// Если есть значение ключа, значит блокировка занята, придется ждать
		if len(w.Data(ukey)) > 0 {
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

func (t *tx64) isCommitted(local *txCache, r db.Reader, txid suid) (_ bool, err error) {
	var status byte

	if status, err = t.txStatus(local, r, txid); err != nil {
		return
	}

	return status == txStatusCommitted, nil
}

func (t *tx64) isAborted(local *txCache, r db.Reader, txid suid) (_ bool, err error) {
	var status byte

	if status, err = t.txStatus(local, r, txid); err != nil {
		return
	}

	return status == txStatusAborted, nil
}

func (t *tx64) txStatus(local *txCache, r db.Reader, txid suid) (status byte, err error) {
	// Дешевле всего чекнуть в глобальном кеше, вдруг уже знаем такую
	if status = globCache.get(txid); status != txStatusUnknown {
		return
	}

	// Возможно, это открытая транзакция из локального кеша
	if status = local.get(txid); status != txStatusUnknown {
		return
	}

	// Придется слазать в БД за статусом и положить в кеш
	val := r.Data(WrapTxKey(txid[:]))

	// Если в БД нет записи, то либо транзакция еще открыта, либо это был откат
	// Поскольку нет определенности, в глобальный кеш ничего не складываем
	if len(val) == 0 {
		local.set(txid, txStatusRunning)
		return txStatusRunning, nil
	}

	// Получаем значение статуса как часть модели
	status = models.GetRootAsTransaction(val, 0).Status()

	// В случае финальных статусов можем положить в глобальный кеш
	if status == txStatusCommitted || status == txStatusAborted {
		globCache.set(txid, status)
	}

	// В локальный кеш можем положить в любом случае, затем вернуть
	local.set(txid, status)
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
	pair := fdb.KeyValue{WrapTxKey(t.txid[:]), t.pack()}
	hdlr := func(w db.Writer) error { w.Upsert(pair); return nil }

	// Выполняем обработчик в физической транзакции - указанной или новой
	if w.Empty() {
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
func (t *tx64) isVisible(r db.Reader, lc *txCache, opid uint32, item fdb.KeyValue, dirty bool) (ok bool, err error) {
	var comm, fail bool

	// Нужна защита от паники, на случай левых или битых записей
	defer func() {
		if rec := recover(); rec != nil {
			glog.Errorf("panic mvcc.tx64.isVisible(%s): %+v", item.Key, rec)
			ok = false
			err = errx.ErrInternal.WithDebug(errx.Debug{
				"key":   item.Key,
				"panic": rec,
			})
		}
	}()

	var xmin suid
	kidx := len(item.Key) - 16
	copy(xmin[:8], item.Key[kidx:kidx+8])
	copy(xmin[8:12], item.Key[kidx+12:kidx+16])
	cmin := binary.BigEndian.Uint32(item.Key[kidx+8 : kidx+12])
	row := models.GetRootAsRow(item.Value, 0).UnPack()

	if Debug {
		defer func() { glog.Infof("isVisible(%s) = %t", item.Key, ok) }()
	}

	// Частный случай - если запись создана в рамках текущей транзакции
	// то даже если запись создана позже, тут возвращаем, чтобы можно было выполнить
	// "превентивное" удаление и чтобы не было никаких конфликтующих строк
	// Поэтому проверять будем только сторонние транзакции
	if xmin == t.txid {
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
		if bytes.Equal(row.Drop[i].Tx, t.txid[:]) {
			// Если удалена до этого момента, то уже не видна
			// Если "позже" в параллельном потоке - то еще можно читать
			return row.Drop[i].Op > opid, nil
		}

		// Проверим, была ли завершена транзакция удаления этой записи
		var dtx suid
		copy(dtx[:], row.Drop[i].Tx)
		if comm, err = t.isCommitted(lc, r, dtx); err != nil {
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
	lg fdb.RangeResult,
	dirty bool,
	exact int,
) (res []fdb.KeyValue, err error) {
	var ok bool

	list := lg.GetSliceOrPanic()
	res = list[:0]
	//res = make([]fdb.KeyValue, 0, len(list))

	// Проверяем все версии, пока не получим актуальную
	for i := range list {
		list[i].Key = list[i].Key[1:]

		if exact > 0 && len(list[i].Key) != (exact+16) {
			continue
		}

		if ok, err = t.isVisible(r, lc, opid, list[i], dirty); err != nil {
			return
		}

		if ok {
			res = append(res, list[i])
		}
	}

	// Подтираем хвост списка, чтобы за ним сборщик мусора пришел
	list = list[:len(res)]
	return res, nil
}

func (t *tx64) dropRows(w db.Writer, opid uint32, pairs []fdb.KeyValue, onDelete RowHandler, physical bool) (err error) {
	var row *models.RowT

	if len(pairs) == 0 {
		return nil
	}

	atomic.AddUint32(&t.mods, 1)

	for _, pair := range pairs {

		if buf := pair.Value; len(buf) > 0 {
			row = models.GetRootAsRow(buf, 0).UnPack()

			// Обработчик имеет возможность предотвратить удаление/обновление, выбросив ошибку
			if onDelete != nil {
				if err = onDelete(t, w, fdb.KeyValue{UnwrapKey(pair.Key), row.Data}); err != nil {
					return ErrDelete.WithReason(err)
				}
			}

			row.Drop = append(row.Drop, &models.TxPtrT{
				Tx: t.txid[:],
				Op: opid,
			})
		} else {
			// По идее сюда никогда не должны попасть, но на всякий случай - элемент автовосстановления стабильности
			row = &models.RowT{
				Drop: []*models.TxPtrT{{
					Tx: t.txid[:],
					Op: opid,
				}},
				Data: nil,
			}
		}

		// Обновляем данные в БД по ключу
		if physical {
			w.Delete(pair.Key)
		} else {
			w.Upsert(fdb.KeyValue{pair.Key, fdbx.FlatPack(row)})
		}
	}
	return nil
}

func (t *tx64) Touch(key fdb.Key) {
	t.OnCommit(func(w db.Writer) error {
		w.Upsert(fdb.KeyValue{WrapWatchKey(key), fdbx.Time2Byte(time.Now())})
		return nil
	})
}

func (t *tx64) Watch(key fdb.Key) (wait fdbx.Waiter, err error) {
	return wait, t.conn.Write(func(w db.Writer) error {
		wait = w.Watch(WrapWatchKey(key))
		return nil
	})
}

func (t *tx64) Vacuum(prefix fdb.Key, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	skip := false
	opts := getOpts(args)
	from := WrapKey(prefix)
	to := WrapKey(prefix)

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
		if len(from) == 0 {
			return nil
		}
		skip = true

		// Передышка, чтобы не слишком грузить бд
		time.Sleep(10 * time.Millisecond)
	}
}

// vacuumPart - функция обратная fetchRows, в том смысле, что она удаляет все ключи, которые больше не нужны в БД
func (t *tx64) vacuumPart(w db.Writer, lg fdb.RangeResult, onVacuum RowHandler) (last fdb.Key, err error) {
	var ok bool

	atomic.AddUint32(&t.mods, 1)

	lc := makeCache()
	rows := 0
	iter := lg.Iterator()
	opid := atomic.AddUint32(&t.opid, 1)
	wctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()

	for iter.Advance() {
		item := iter.MustGet()
		item.Key = item.Key[1:]
		last = item.Key
		rows++

		if ok, err = t.isVisible(w.Reader, lc, opid, item, true); err != nil {
			// Ошибку игнорим, потому что это вакуум, важно не удалить лишнего
			continue
		}

		if !ok {
			if onVacuum != nil {
				if err = onVacuum(t, w, usrPair(item)); err != nil {
					return
				}
			}

			w.Delete(item.Key)
		}

		if wctx.Err() != nil {
			return
		}
	}

	// Больше нечего получить - условие выхода
	if rows == 0 {
		return nil, nil
	}

	// Возвращаем последний проверенный ключ, с которого надо начать след. цикл
	return last, nil
}

type locks struct {
	sync.Mutex
	acks map[string]fdb.Key
}

func (l *locks) Append(key fdb.Key) (exists bool) {
	l.Lock()
	defer l.Unlock()

	if l.acks == nil {
		l.acks = make(map[string]fdb.Key, 1)
	}

	if l.acks[key.String()] != nil {
		return true
	}

	l.acks[key.String()] = key
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

	if w.Empty() {
		return t.conn.Write(hdlr)
	}

	return hdlr(w)
}
