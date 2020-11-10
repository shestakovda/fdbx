package mvcc

import (
	"context"
	"encoding/binary"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
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
func newTx64(conn db.Connection) (*tx64, error) {
	var waiter fdb.FutureKey

	if err := conn.Write(func(w db.Writer) error {
		w.Increment(counterKey, 1)
		waiter = w.Version()
		return nil
	}); err != nil {
		return nil, ErrBegin.WithReason(err)
	}

	ver := waiter.MustGet()

	if len(ver) < 10 {
		return nil, ErrBegin.WithDebug(errx.Debug{
			"version": fdbx.Key(ver).String(),
		})
	}

	return &tx64{
		txid:   binary.BigEndian.Uint64(ver[:8]) + uint64(binary.BigEndian.Uint16(ver[8:10])),
		conn:   conn,
		start:  uint64(time.Now().UTC().UnixNano()),
		status: txStatusRunning,
	}, nil
}

type tx64 struct {
	conn   db.Connection
	mods   uint32
	opid   uint32
	txid   uint64
	start  uint64
	status byte
	oncomm []CommitHandler
}

func (t tx64) Conn() db.Connection { return t.conn }

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
		lc := makeCache()
		cp := make([]fdbx.Key, len(keys))
		lg := make([]fdbx.ListGetter, len(keys))

		for i := range keys {
			cp[i] = keyMgr.Wrap(keys[i])
			lg[i] = w.List(cp[i], cp[i], 0, true)
		}

		var row fdbx.Pair

		for i := range cp {
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
	if len(pairs) == 1 {
		return t.upsertOne(pairs[0], args)
	}

	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	wrap := t.packWrapper(opid)
	hdlr := func(w db.Writer) (exp error) {
		var ukey fdbx.Key
		var row fdbx.Pair

		lc := makeCache()
		cp := make([]fdbx.Pair, len(pairs))
		lg := make([]fdbx.ListGetter, len(pairs))

		for i := range pairs {
			cp[i] = pairs[i].Clone().WrapKey(keyMgr.Wrapper)

			if ukey, exp = cp[i].Key(); exp != nil {
				return
			}

			lg[i] = w.List(ukey, ukey, 0, true)
		}

		for i := range cp {
			if row, exp = t.fetchRow(w, lc, opid, lg[i]); exp != nil {
				return
			}

			if exp = t.dropRow(w, opid, row, opts.onDelete); exp != nil {
				return
			}

			if exp = w.Upsert(cp[i].WrapKey(t.txWrapper).WrapValue(wrap)); exp != nil {
				return
			}

			if opts.onInsert == nil {
				continue
			}

			if exp = opts.onInsert(t, pairs[i]); exp != nil {
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
		return ErrUpsert.WithReason(err)
	}

	return nil
}

func (t *tx64) upsertOne(pair fdbx.Pair, args []Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	wrap := t.packWrapper(opid)
	hdlr := func(w db.Writer) (exp error) {
		var ukey fdbx.Key
		var row fdbx.Pair

		cp := pair.Clone().WrapKey(keyMgr.Wrapper)

		if ukey, exp = cp.Key(); exp != nil {
			return
		}

		if row, exp = t.fetchRow(w, nil, opid, w.List(ukey, ukey, 0, true)); exp != nil {
			return
		}

		if exp = t.dropRow(w, opid, row, opts.onDelete); exp != nil {
			return
		}

		if exp = w.Upsert(cp.WrapKey(t.txWrapper).WrapValue(wrap)); exp != nil {
			return
		}

		if opts.onInsert != nil {
			if exp = opts.onInsert(t, pair); exp != nil {
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
		return ErrUpsert.WithReason(err)
	}

	return nil
}

/*
	Select - выборка актуального в данной транзакции значения ключа.
*/
func (t *tx64) Select(key fdbx.Key) (res fdbx.Pair, err error) {
	ukey := keyMgr.Wrap(key)
	opid := atomic.AddUint32(&t.opid, 1)

	if err = t.conn.Read(func(r db.Reader) (exp error) {
		var row fdbx.Pair

		if row, exp = t.fetchRow(r, nil, opid, r.List(ukey, ukey, 0, true)); exp != nil {
			return
		}

		if row != nil {
			res = row.WrapKey(keyMgr.Unwrapper).WrapValue(valWrapper)
			return nil
		}

		return ErrNotFound.WithDebug(errx.Debug{
			"key": key.String(),
		})
	}); err != nil {
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
					errs <- ErrSeqScan.WithReason(ctx.Err())
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
		opts := getOpts(args)
		from := keyMgr.Wrap(opts.from)
		last := keyMgr.Wrap(opts.to)
		opid := atomic.AddUint32(&t.opid, 1)
		hdlr := func(r db.Reader) (exp error) {
			if opts.reverse {
				part, last, exp = t.selectPart(r, from, last, size, opid, &opts)
			} else {
				part, from, exp = t.selectPart(r, from, last, size, opid, &opts)
			}
			size += len(part)
			return exp
		}

		for {
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
				return
			}

			select {
			case list <- part:
				size += len(part)
			case <-ctx.Done():
				errs <- ErrSeqScan.WithReason(ctx.Err())
				return
			}

			if opts.limit > 0 && size >= opts.limit {
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
	opid uint32,
	opts *options,
) (part []fdbx.Pair, last fdbx.Key, err error) {
	var ok bool
	var w db.Writer
	var key fdbx.Key

	if part, err = t.fetchAll(r, nil, opid, r.List(from, to, MaxRowCount, opts.reverse)); err != nil {
		return
	}

	if len(part) == 0 {
		return nil, from, nil
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

	if opts.reverse {
		if key, err = part[0].Key(); err != nil {
			return
		}
		last = key
	} else {
		if key, err = part[cnt-1].Key(); err != nil {
			return
		}
		last = key.RPart(0x01)
	}

	for i := range part {
		part[i] = part[i].WrapKey(keyMgr.Unwrapper).WrapValue(valWrapper)

		if opts.onLock != nil {
			// Раз какой-то обработчик в записи, значит модификация - увеличиваем счетчик
			atomic.AddUint32(&t.mods, 1)

			if err = opts.onLock(t, part[i].Clone(), w); err != nil {
				return
			}
		}
	}

	return part, last, nil
}

func (t *tx64) SaveBLOB(key fdbx.Key, blob []byte, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	sum := 0
	tmp := blob
	num := uint16(0)
	prs := make([]fdbx.Pair, 0, MaxRowMem/MaxRowSize+1)
	wrp := func(k fdbx.Key) (fdbx.Key, error) {
		return k.RPart(byte(num>>8), byte(num)), nil
	}

	set := func(w db.Writer) (exp error) {
		for i := range prs {
			if exp = w.Upsert(prs[i].WrapKey(keyMgr.Wrapper).WrapKey(wrp)); exp != nil {
				return
			}
			num++
		}
		return nil
	}

	// Разбиваем на элементарные значения, пишем пачками по 10 мб
	for {
		if len(tmp) > MaxRowSize {
			prs = append(prs, fdbx.NewPair(key, tmp[:MaxRowSize]))
			tmp = tmp[MaxRowSize:]
			sum += MaxRowSize
		} else {
			prs = append(prs, fdbx.NewPair(key, tmp))
			sum += len(tmp)
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
	var val []byte
	var rows []fdbx.Pair

	ukey := keyMgr.Wrap(key)
	end := ukey.RPart(0xFF, 0xFF)
	res := make([]byte, 0, size)

	for {
		if err = t.conn.Read(func(r db.Reader) (exp error) {
			rows = r.List(ukey, end, 10, false).Resolve()
			return nil
		}); err != nil {
			return nil, ErrBLOBLoad.WithReason(err)
		}

		if len(rows) == 0 {
			break
		}

		for i := range rows {
			if val, err = rows[i].Value(); err != nil {
				return nil, ErrBLOBLoad.WithReason(err)
			}

			res = append(res, val...)
		}

		if ukey, err = rows[len(rows)-1].Key(); err != nil {
			return nil, ErrBLOBLoad.WithReason(err)
		}
		ukey = ukey.RPart(0x01)
	}

	return res, nil
}

func (t *tx64) DropBLOB(key fdbx.Key, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	ukey := keyMgr.Wrap(key)
	hdlr := func(w db.Writer) error {
		w.Erase(ukey, ukey)
		return nil
	}

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
	var val []byte

	if val, err = r.Data(txKey(x)).Value(); err != nil {
		return
	}

	// Если в БД нет записи, то либо транзакция еще открыта, либо это был откат
	// Поскольку нет определенности, в кеш ничего не складываем
	if len(val) == 0 {
		return txStatusAborted, nil
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

func (t *tx64) packWrapper(opid uint32) fdbx.ValueWrapper {
	state := &models.RowStateT{
		XMin: t.txid,
		CMin: opid,
	}
	return func(v []byte) ([]byte, error) {
		return fdbx.FlatPack(&models.RowT{
			State: state,
			Data:  v,
		}), nil
	}
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

	// Получаем ключ транзакции и устанавливаем статус
	txid := txKey(t.txid)
	mods := atomic.LoadUint32(&t.mods)
	t.status = status

	// Если это откат - то сохраняем данные только в случае, если
	// в рамках транзакции могли быть какие-то изменения (счетчик mods > 0)
	// В остальных случаях можем спокойно установить локальный кеш и выйти
	if t.status == txStatusAborted && mods == 0 {
		globCache.set(t.txid, t.status)
		return nil
	}

	// Cохраняем в БД объект с обновленным статусом
	pair := fdbx.NewPair(txid, t.pack())
	hdlr := func(w db.Writer) error { return w.Upsert(pair) }

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
	var val []byte
	var buf models.RowState
	var row models.RowStateT

	if lc == nil {
		lc = makeCache()
	}

	// Загружаем список версий, существующих в рамках этой операции
	list := lg.Resolve()

	// Проверяем все версии, пока не получим актуальную
	for i := range list {
		if val, err = list[i].Value(); err != nil {
			return
		}

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
func (t *tx64) fetchAll(r db.Reader, lc *txCache, opid uint32, lg fdbx.ListGetter) (res []fdbx.Pair, err error) {
	var comm bool
	var val []byte
	var buf models.RowState
	var row models.RowStateT

	if lc == nil {
		lc = makeCache()
	}

	list := lg.Resolve()
	res = list[:0]

	// Проверяем все версии, пока не получим актуальную
	for i := range list {
		if val, err = list[i].Value(); err != nil {
			return
		}

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

	return res, nil
}

func (t *tx64) dropRow(w db.Writer, opid uint32, pair fdbx.Pair, onDelete Handler) (err error) {
	if pair == nil {
		return nil
	}

	var key fdbx.Key
	var data []byte

	atomic.AddUint32(&t.mods, 1)

	w.Upsert(pair.WrapValue(func(v []byte) ([]byte, error) {
		if len(v) > 0 {
			var state models.RowState

			row := models.GetRootAsRow(v, 0)

			if onDelete != nil {
				data = row.DataBytes()
			}

			row.State(&state)

			// В модели состояния поля указаны как required
			// Обновление должно произойти 100%, если только это не косяк модели или баг библиотеки
			if !(state.MutateXMax(t.txid) && state.MutateCMax(opid)) {
				return nil, ErrUpsert.WithStack()
			}
		}

		return v, nil
	}))

	if onDelete == nil {
		return nil
	}

	if key, err = pair.Key(); err != nil {
		return
	}

	if err = onDelete(t, fdbx.NewPair(key, data).WrapKey(keyMgr.Unwrapper)); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (t *tx64) txWrapper(key fdbx.Key) (fdbx.Key, error) {
	if key == nil {
		return nil, nil
	}

	var txid [8]byte
	binary.BigEndian.PutUint64(txid[:], t.txid)
	return key.RPart(txid[:]...), nil
}

func (t *tx64) Vacuum(prefix fdbx.Key, args ...Option) (err error) {
	atomic.AddUint32(&t.mods, 1)

	opts := getOpts(args)
	from := keyMgr.Wrap(prefix)
	to := from.Clone()

	for {
		if err = t.conn.Write(func(w db.Writer) (exp error) {
			lg := w.List(from, to, uint64(opts.packSize), false)

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
	}
}

// vacuumPart - функция обратная fetchAll, в том смысле, что она удаляет все ключи, которые больше не нужны в БД
func (t *tx64) vacuumPart(w db.Writer, lg fdbx.ListGetter, onVacuum RowHandler) (_ fdbx.Key, err error) {
	var ok bool
	var val []byte
	var key fdbx.Key
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
		if val, err = list[i].Value(); err != nil {
			return
		}

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
			if err = onVacuum(t, drop[i].Clone().WrapKey(keyMgr.Unwrapper).WrapValue(valWrapper), w); err != nil {
				return
			}
		}

		if key, err = drop[i].Key(); err != nil {
			return
		}

		w.Delete(key)
	}

	// Возвращаем последний проверенный ключ, с которого надо начать след. цикл
	if key, err = list[len(list)-1].Key(); err != nil {
		return
	}

	return key.RPart(0x01), nil
}
