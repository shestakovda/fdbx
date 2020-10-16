package mvcc

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	fbs "github.com/google/flatbuffers/go"
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/typex"
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
		oncomm: hdlPool.Get().([]CommitHandler),
	}

	key := fdbx.Key(typex.NewUUID()).LPart(nsTxTmp)

	if err = conn.Write(func(w db.Writer) error {
		w.Versioned(key)
		return nil
	}); err != nil {
		return nil, ErrBegin.WithReason(err)
	}

	if err = conn.Write(func(w db.Writer) (exp error) {
		var val []byte

		if val, exp = w.Data(key).Value(); exp != nil {
			return
		}

		t.txid = binary.BigEndian.Uint64(val[:8])
		w.Upsert(fdbx.NewPair(fdbx.Key(val[:8]).LPart(nsTx), t.pack()))
		w.Delete(key)
		return nil
	}); err != nil {
		return nil, ErrBegin.WithReason(err)
	}

	return t, nil
}

type tx64 struct {
	conn   db.Connection
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

	if err = t.close(txStatusCommitted, opts.writer); err != nil {
		return
	}

	globCache.set(t.txid, txStatusCommitted)
	return nil
}

func (t *tx64) Cancel(args ...Option) (err error) {
	opts := getOpts(args)

	if err = t.close(txStatusAborted, opts.writer); err != nil {
		return
	}

	globCache.set(t.txid, txStatusAborted)
	return nil
}

/*
	Delete - удаление актуальной в данный момент записи, если она существует.

	Чтобы найти актуальную запись, нужно сделать по сути обычный Select.
	Удалить - значит обновить значение в служебных полях и записать в тот же ключ.

	Важно, чтобы выборка и обновление шли строго в одной внутренней FDB транзакции.
*/
func (t *tx64) Delete(keys []fdbx.Key, args ...Option) (err error) {
	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	hdlr := func(w db.Writer) (exp error) {
		lc := makeCache()
		cp := make([]fdbx.Key, len(keys))
		lg := make([]fdbx.ListGetter, len(keys))

		for i := range keys {
			cp[i] = usrKey(keys[i])
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
	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	hdlr := func(w db.Writer) (exp error) {
		var ukey fdbx.Key
		var row fdbx.Pair

		lc := makeCache()
		cp := make([]fdbx.Pair, len(pairs))
		lg := make([]fdbx.ListGetter, len(pairs))

		for i := range pairs {
			cp[i] = pairs[i].WrapKey(usrWrapper)

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

			if exp = w.Upsert(cp[i].WrapKey(t.txWrapper).WrapValue(t.packWrapper(opid))); exp != nil {
				return
			}

			if opts.onInsert == nil {
				continue
			}

			if exp = opts.onInsert(t, cp[i].WrapKey(sysWrapper)); exp != nil {
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
	ukey := usrKey(key)
	opid := atomic.AddUint32(&t.opid, 1)

	if err = t.conn.Read(func(r db.Reader) (exp error) {
		var row fdbx.Pair

		if row, exp = t.fetchRow(r, nil, opid, r.List(ukey, ukey, 0, true)); exp != nil {
			return
		}

		if row != nil {
			res = row.WrapKey(sysWrapper).WrapValue(valWrapper)
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

func (t *tx64) SeqScan(from, to fdbx.Key, args ...Option) (_ []fdbx.Pair, err error) {
	size := 0
	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	list := make([]fdbx.Pair, 0, 64)
	part := make([]fdbx.Pair, 0, 64)
	hdlr := func(r db.Reader) (exp error) {
		var ok bool
		var w db.Writer
		var key fdbx.Key
		var rows []fdbx.Pair

		if rows, exp = t.fetchAll(r, nil, opid, r.List(from, to, uint64(opts.packSize), false)); exp != nil {
			return
		}

		if opts.lock {
			if w, ok = r.(db.Writer); ok {
				w.Lock(from, to)
			}
		}

		if len(rows) == 0 {
			part = part[:0]
			return nil
		}

		cnt := len(rows)

		if opts.limit > 0 && (size+cnt) > opts.limit {
			cnt = opts.limit - size
		}

		part = rows[:cnt]

		for i := range part {
			part[i] = part[i].WrapKey(sysWrapper).WrapValue(valWrapper)

			if opts.onLock != nil {
				if exp = opts.onLock(t, part[i], w); exp != nil {
					return
				}
			}
		}

		size += len(part)

		if key, exp = part[len(part)-1].Key(); exp != nil {
			return
		}

		from = usrKey(key).RPart(0x01)
		return nil
	}

	from = usrKey(from)
	to = usrKey(to)

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
			return nil, ErrSeqScan.WithReason(err)
		}

		if len(part) == 0 {
			break
		}

		list = append(list, part...)

		if opts.limit > 0 && len(list) >= opts.limit {
			break
		}
	}

	return list, nil
}

func (t *tx64) SaveBLOB(key fdbx.Key, blob []byte) (err error) {
	sum := 0
	tmp := blob
	num := uint16(0)
	prs := make([]fdbx.Pair, 0, txLimit/loLimit+1)
	wrp := func(k fdbx.Key) (fdbx.Key, error) {
		return k.RPart(byte(num>>8), byte(num)), nil
	}

	set := func(w db.Writer) (exp error) {
		for i := range prs {
			if exp = w.Upsert(prs[i].WrapKey(usrWrapper).WrapKey(wrp)); exp != nil {
				return
			}
			num++
		}
		return nil
	}

	// Разбиваем на элементарные значения, пишем пачками по 10 мб
	for {
		if len(tmp) > loLimit {
			prs = append(prs, fdbx.NewPair(key, tmp[:loLimit]))
			tmp = tmp[loLimit:]
			sum += loLimit
		} else {
			prs = append(prs, fdbx.NewPair(key, tmp))
			sum += len(tmp)
			break
		}

		if sum >= txLimit-loLimit {
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

func (t *tx64) LoadBLOB(key fdbx.Key, size int) (_ []byte, err error) {
	var val []byte
	var rows []fdbx.Pair

	ukey := usrKey(key)
	end := ukey.RPart(0xFF, 0xFF)
	res := make([]byte, 0, size)

	for {
		if err = t.conn.Read(func(r db.Reader) (exp error) {
			rows = r.List(ukey, end, 10, false)()
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

func (t *tx64) DropBLOB(key fdbx.Key) (err error) {
	ukey := usrKey(key)
	end := ukey.RPart(0xFF, 0xFF)

	if err = t.conn.Write(func(w db.Writer) error {
		w.Erase(ukey, end)
		return nil
	}); err != nil {
		return ErrBLOBDrop.WithReason(err)
	}

	return nil
}

func (t *tx64) isCommitted(local *txCache, r db.Reader, x uint64) (_ bool, err error) {
	var status byte

	// Дешевле всего чекнуть в глобальном кеше, вдруг уже знаем такую
	if status = globCache.get(x); status != txStatusUnknown {
		return status == txStatusCommitted, nil
	}

	// Возможно, это открытая транзакция из локального кеша
	if status = local.get(x); status != txStatusUnknown {
		return status == txStatusCommitted, nil
	}

	// Придется слазать в БД за статусом и положить в кеш
	var val []byte

	if val, err = r.Data(txKey(x)).Value(); err != nil {
		return
	}

	if len(val) == 0 {
		return false, nil
	}

	// Получаем значение статуса как часть модели
	status = models.GetRootAsTransaction(val, 0).Status()

	// В случае финальных статусов можем положить в глобальный кеш
	if status == txStatusCommitted || status == txStatusAborted {
		globCache.set(x, status)
	}

	// В локальный кеш можем положить в любом случае, затем вернуть
	local.set(x, status)
	return status == txStatusCommitted, nil
}

func (t *tx64) pack() []byte {
	tx := &models.TransactionT{
		TxID:   t.txid,
		Start:  t.start,
		Status: t.status,
	}

	buf := fbsPool.Get().(*fbs.Builder)
	buf.Finish(tx.Pack(buf))
	res := buf.FinishedBytes()
	buf.Reset()
	fbsPool.Put(buf)
	return res
}

func (t *tx64) packWrapper(opid uint32) fdbx.ValueWrapper {
	return func(v []byte) ([]byte, error) {
		row := &models.RowT{
			State: &models.RowStateT{
				XMin: t.txid,
				CMin: opid,
			},
			Data: v,
		}

		buf := fbsPool.Get().(*fbs.Builder)
		buf.Finish(row.Pack(buf))
		res := buf.FinishedBytes()
		buf.Reset()
		fbsPool.Put(buf)
		return res, nil
	}
}

func (t *tx64) close(status byte, w db.Writer) (err error) {
	if t.status == txStatusCommitted || t.status == txStatusAborted {
		return nil
	}
	defer hdlPool.Put(t.oncomm[:0])

	t.status = status
	dump := t.pack()
	txid := txKey(t.txid)
	hdlr := func(w db.Writer) (exp error) { return w.Upsert(fdbx.NewPair(txid, dump)) }

	if w == nil {
		err = t.conn.Write(hdlr)
	} else {
		err = hdlr(w)
	}

	if err != nil {
		return ErrClose.WithReason(err)
	}

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
	list := lg()

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

	list := lg()
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

	if err = onDelete(t, fdbx.NewPair(key, data).WrapKey(sysWrapper)); err != nil {
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
