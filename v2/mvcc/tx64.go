package mvcc

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	fbs "github.com/google/flatbuffers/go"
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
	}

	key := fdbx.Key(typex.NewUUID()).LPart(nsTxTmp)

	if err = conn.Write(func(w db.Writer) error {
		w.Versioned(key)
		return nil
	}); err != nil {
		return nil, ErrBegin.WithReason(err)
	}

	if err = conn.Write(func(w db.Writer) (exp error) {
		var val fdbx.Value
		var pair fdbx.Pair

		if pair, exp = w.Data(key); exp != nil {
			return
		}

		if val, exp = pair.Value(); exp != nil {
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
}

func (t *tx64) Commit() (err error) {
	if err = t.close(txStatusCommitted); err != nil {
		return
	}

	globCache.set(t.txid, txStatusCommitted)
	return nil
}

func (t *tx64) Cancel() (err error) {
	if err = t.close(txStatusAborted); err != nil {
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

	if err = t.conn.Write(func(w db.Writer) (exp error) {
		lc := makeCache()
		cp := make([]fdbx.Key, len(keys))
		lg := make([]db.ListGetter, len(keys))

		for i := range keys {
			if cp[i], exp = usrWrapper(keys[i]); exp != nil {
				return
			}
			if lg[i], exp = w.List(cp[i], cp[i], 0, true); exp != nil {
				return
			}
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
	}); err != nil {
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

	if err = t.conn.Write(func(w db.Writer) (exp error) {
		var ukey fdbx.Key
		var row fdbx.Pair

		lc := makeCache()
		cp := make([]fdbx.Pair, len(pairs))
		lg := make([]db.ListGetter, len(pairs))

		for i := range pairs {
			cp[i] = pairs[i].WrapKey(usrWrapper)

			if ukey, exp = cp[i].Key(); exp != nil {
				return
			}

			if lg[i], exp = w.List(ukey, ukey, 0, true); exp != nil {
				return
			}
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

			if exp = opts.onInsert(t, cp[i]); exp != nil {
				return
			}
		}

		return nil
	}); err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

/*
	Select - выборка актуального в данной транзакции значения ключа.
*/
func (t *tx64) Select(key fdbx.Key) (res fdbx.Pair, err error) {
	var ukey fdbx.Key
	opid := atomic.AddUint32(&t.opid, 1)

	if ukey, err = usrWrapper(key); err != nil {
		return
	}

	if err = t.conn.Read(func(r db.Reader) (exp error) {
		var row fdbx.Pair
		var lgt db.ListGetter

		if lgt, exp = r.List(ukey, ukey, 0, true); exp != nil {
			return
		}

		if row, exp = t.fetchRow(r, nil, opid, lgt); exp != nil {
			return
		}

		if row != nil {
			res = row.WrapValue(valWrapper)
		} else {
			res = fdbx.NewPair(key, nil)
		}

		return nil
	}); err != nil {
		return nil, ErrSelect.WithReason(err)
	}

	return res, nil
}

func (t *tx64) SeqScan(from, to fdbx.Key) (res []fdbx.Pair, err error) {
	size := 0
	next := true
	opid := atomic.AddUint32(&t.opid, 1)
	list := make([][]fdbx.Pair, 0, 64)

	if from, err = usrWrapper(from); err != nil {
		return
	}

	if to, err = usrWrapper(to); err != nil {
		return
	}

	for next {
		if err = t.conn.Read(func(r db.Reader) (exp error) {
			var key fdbx.Key
			var lgt db.ListGetter
			var rows []fdbx.Pair

			if lgt, exp = r.List(from, to, ScanRangeSize, false); exp != nil {
				return
			}

			if rows, exp = t.fetchAll(r, nil, opid, lgt); exp != nil {
				return
			}

			switch len(rows) {
			case 0:
				next = false
			case 1:
				list = append(list, []fdbx.Pair{rows[0].WrapValue(valWrapper)})
				next = false
				size++
			default:
				cnt := len(rows) - 1

				if key, exp = rows[cnt].Key(); exp != nil {
					return
				}

				if from, exp = usrWrapper(key); exp != nil {
					return
				}

				for i := 0; i < cnt; i++ {
					rows[i] = rows[i].WrapValue(valWrapper)
				}

				list = append(list, rows[:cnt])
				size += cnt
			}
			return nil
		}); err != nil {
			return nil, ErrSeqScan.WithReason(err)
		}
	}

	res = make([]fdbx.Pair, 0, size)

	for i := range list {
		res = append(res, list[i]...)
	}

	return res, nil
}

func (t *tx64) SaveBLOB(blob fdbx.Value) (_ fdbx.Key, err error) {
	sum := 0
	tmp := blob
	num := uint16(0)
	uid := typex.NewUUID()
	key := fdbx.Key(uid).LPart(nsBLOB)
	prs := make([]fdbx.Pair, 0, txLimit/loLimit+1)
	wrp := func(key fdbx.Key) (fdbx.Key, error) {
		return key.RPart(byte(num>>8), byte(num)), nil
	}

	set := func(w db.Writer) (exp error) {
		for i := range prs {
			if exp = w.Upsert(prs[i].WrapKey(wrp)); exp != nil {
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

		if sum > txLimit-loLimit {
			if err = t.conn.Write(set); err != nil {
				return nil, ErrBLOBSave.WithReason(err)
			}
			sum = 0
			prs = prs[:0]
		}
	}

	if len(prs) > 0 {
		if err = t.conn.Write(set); err != nil {
			return nil, ErrBLOBSave.WithReason(err)
		}
	}

	return fdbx.Key(uid), nil
}

func (t *tx64) LoadBLOB(uid fdbx.Key, size uint32) (_ fdbx.Value, err error) {
	var val fdbx.Value
	var rows []fdbx.Pair

	key := uid.LPart(nsBLOB)
	end := key.RPart(0xFF, 0xFF)
	res := make(fdbx.Value, 0, size)

	for {
		if err = t.conn.Read(func(r db.Reader) (exp error) {
			var lsg db.ListGetter

			if lsg, exp = r.List(key, end, 10, false); exp != nil {
				return
			}

			rows = lsg()
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

		if key, err = rows[len(rows)-1].Key(); err != nil {
			return nil, ErrBLOBLoad.WithReason(err)
		}
		key = key.RPart(0x01)
	}

	return res, nil
}

func (t *tx64) DropBLOB(uid fdbx.Key) (err error) {
	key := uid.LPart(nsBLOB)
	end := key.RPart(0xFF, 0xFF)

	if err = t.conn.Write(func(w db.Writer) error {
		w.Erase(key, end)
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
	var val fdbx.Value
	var pair fdbx.Pair

	if pair, err = r.Data(txKey(x)); err != nil {
		return
	}

	if val, err = pair.Value(); err != nil {
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
	return func(v fdbx.Value) (fdbx.Value, error) {
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

func (t *tx64) close(status byte) (err error) {
	if t.status == txStatusCommitted || t.status == txStatusAborted {
		return nil
	}

	t.status = status
	dump := t.pack()
	txid := txKey(t.txid)

	if err = t.conn.Write(func(w db.Writer) (exp error) {
		w.Upsert(fdbx.NewPair(txid, dump))
		return nil
	}); err != nil {
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
func (t *tx64) fetchRow(r db.Reader, lc *txCache, opid uint32, lg db.ListGetter) (_ fdbx.Pair, err error) {
	var comm bool
	var val fdbx.Value
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
			return list[i].WrapKey(sysWrapper), nil
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
			return list[i].WrapKey(sysWrapper), nil
		default:
			// Если запись была удалена другой транзакцией, но она еще закоммичена, то еще видна
			if comm, err = t.isCommitted(lc, r, row.XMax); err != nil {
				return
			}

			if !comm {
				return list[i].WrapKey(sysWrapper), nil
			}
		}
	}

	return nil, nil
}

// Аналогично fetchRow, но все актуальные версии всех ключей по диапазону
func (t *tx64) fetchAll(r db.Reader, lc *txCache, opid uint32, lg db.ListGetter) (res []fdbx.Pair, err error) {
	var comm bool
	var val fdbx.Value
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
			res = append(res, list[i].WrapKey(sysWrapper))
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
			res = append(res, list[i].WrapKey(sysWrapper))
			continue
		default:
			// Если запись была удалена другой транзакцией, но она еще закоммичена, то еще видна
			if comm, err = t.isCommitted(lc, r, row.XMax); err != nil {
				return
			}
			if !comm {
				res = append(res, list[i].WrapKey(sysWrapper))
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
	var data fdbx.Value

	w.Upsert(pair.WrapKey(usrWrapper).WrapValue(func(v fdbx.Value) (fdbx.Value, error) {
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

	if err = onDelete(t, fdbx.NewPair(key, data)); err != nil {
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
