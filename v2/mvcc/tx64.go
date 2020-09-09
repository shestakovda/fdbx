package mvcc

import (
	"encoding/binary"
	"sync/atomic"
	"time"

	fbs "github.com/google/flatbuffers/go"
	"github.com/google/uuid"
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

	uid := uuid.New()
	key := fdbx.Key(uid[:]).LPart(nsTxTmp)

	if err = conn.Write(func(w db.Writer) error {
		w.Versioned(key)
		return nil
	}); err != nil {
		return nil, ErrBegin.WithReason(err)
	}

	if err = conn.Write(func(w db.Writer) error {
		ver := w.Data(key).Value()[:8]
		t.txid = binary.BigEndian.Uint64(ver)
		w.Upsert(fdbx.NewPair(fdbx.Key(ver).LPart(nsTx), t.pack()))
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
			cp[i] = usrWrapper(keys[i])
			lg[i] = w.List(cp[i], cp[i], 0, true)
		}

		for i := range cp {
			if exp = t.dropRow(w, opid, t.fetchRow(w, lc, opid, lg[i]), opts.onDelete); exp != nil {
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
		lc := makeCache()
		cp := make([]fdbx.Pair, len(pairs))
		lg := make([]db.ListGetter, len(pairs))

		for i := range pairs {
			cp[i] = pairs[i].WrapKey(usrWrapper)
			ukey := cp[i].Key()
			lg[i] = w.List(ukey, ukey, 0, true)
		}

		for i := range cp {
			if exp = t.dropRow(w, opid, t.fetchRow(w, lc, opid, lg[i]), opts.onDelete); exp != nil {
				return
			}

			w.Upsert(cp[i].WrapKey(t.txWrapper).WrapValue(t.packWrapper(opid)))

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
	ukey := usrWrapper(key)
	opid := atomic.AddUint32(&t.opid, 1)

	if err = t.conn.Read(func(r db.Reader) (exp error) {
		pair := t.fetchRow(r, nil, opid, r.List(ukey, ukey, 0, true))

		if pair != nil {
			res = pair.WrapValue(valWrapper)
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
	from = usrWrapper(from)
	to = usrWrapper(to)

	for next {
		if err = t.conn.Read(func(r db.Reader) error {
			pairs := t.fetchAll(r, nil, opid, r.List(from, to, ScanRangeSize, false))

			switch len(pairs) {
			case 0:
				next = false
			case 1:
				list = append(list, []fdbx.Pair{pairs[0].WrapValue(valWrapper)})
				next = false
				size++
			default:
				cnt := len(pairs) - 1
				from = usrWrapper(pairs[cnt].Key())

				for i := 0; i < cnt; i++ {
					pairs[i] = pairs[i].WrapValue(valWrapper)
				}

				list = append(list, pairs[:cnt])
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

func (t *tx64) isCommitted(local *txCache, r db.Reader, x uint64) bool {
	var buf []byte
	var status byte

	// Дешевле всего чекнуть в глобальном кеше, вдруг уже знаем такую
	if status = globCache.get(x); status != txStatusUnknown {
		return status == txStatusCommitted
	}

	// Возможно, это открытая транзакция из локального кеша
	if status = local.get(x); status != txStatusUnknown {
		return status == txStatusCommitted
	}

	// Придется слазать в БД за статусом и положить в кеш
	if buf = r.Data(txKey(x)).Value(); len(buf) == 0 {
		return false
	}

	// Получаем значение статуса как часть модели
	status = models.GetRootAsTransaction(buf, 0).Status()

	// В случае финальных статусов можем положить в глобальный кеш
	if status == txStatusCommitted || status == txStatusAborted {
		globCache.set(x, status)
	}

	// В локальный кеш можем положить в любом случае, затем вернуть
	local.set(x, status)
	return status == txStatusCommitted
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
	return func(v fdbx.Value) fdbx.Value {
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
		return res
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
func (t *tx64) fetchRow(r db.Reader, lc *txCache, opid uint32, lg db.ListGetter) fdbx.Pair {
	var buf models.RowState
	var row models.RowStateT

	if lc == nil {
		lc = makeCache()
	}

	// Загружаем список версий, существующих в рамках этой операции
	list := lg()

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
			return list[i].WrapKey(sysWrapper)
		}

		comm := t.isCommitted(lc, r, row.XMin)

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
			return list[i].WrapKey(sysWrapper)
		default:
			// Если запись была удалена другой транзакцией, но она еще закоммичена, то еще видна
			if !t.isCommitted(lc, r, row.XMax) {
				return list[i].WrapKey(sysWrapper)
			}
		}
	}

	return nil
}

// Аналогично fetchRow, но все актуальные версии всех ключей по диапазону
func (t *tx64) fetchAll(r db.Reader, lc *txCache, opid uint32, lg db.ListGetter) []fdbx.Pair {
	var buf models.RowState
	var row models.RowStateT

	if lc == nil {
		lc = makeCache()
	}

	list := lg()
	res := list[:0]

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
			res = append(res, list[i].WrapKey(sysWrapper))
			continue
		}

		comm := t.isCommitted(lc, r, row.XMin)

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
			if !t.isCommitted(lc, r, row.XMax) {
				res = append(res, list[i].WrapKey(sysWrapper))
				continue
			}
		}
	}

	// Стираем хвост, чтобы сборщик мусора пришел за ним
	for i := len(res); i < len(list); i++ {
		list[i] = nil
	}

	return res
}

func (t *tx64) dropRow(w db.Writer, opid uint32, pair fdbx.Pair, onDelete Handler) (err error) {
	if pair == nil {
		return nil
	}

	var data fdbx.Value

	w.Upsert(pair.WrapKey(usrWrapper).WrapValue(func(v fdbx.Value) fdbx.Value {
		if len(v) > 0 {
			var state models.RowState

			row := models.GetRootAsRow(v, 0)

			if onDelete != nil {
				data = row.DataBytes()
			}

			row.State(&state)

			// В модели состояния поля указаны как required
			// Обновление должно произойти 100%, если только это не косяк модели или баг библиотеки
			// Именно поэтому тут паника. Если это не получилось сделать - использовать приложение нельзя!
			if !(state.MutateXMax(t.txid) && state.MutateCMax(opid)) {
				panic(ErrUpsert.WithStack())
			}
		}

		return v
	}))

	if onDelete == nil {
		return nil
	}

	if err = onDelete(t, fdbx.NewPair(pair.Key(), data)); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (t *tx64) txWrapper(key fdbx.Key) fdbx.Key {
	if key == nil {
		return nil
	}

	var txid [8]byte
	binary.BigEndian.PutUint64(txid[:], t.txid)
	return key.RPart(txid[:]...)
}
