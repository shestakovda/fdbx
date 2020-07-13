package mvcc

import (
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/golang/glog"
	fbs "github.com/google/flatbuffers/go"
	"github.com/google/uuid"
	"github.com/shestakovda/errors"
	"github.com/shestakovda/fdbx/db"
	"github.com/shestakovda/fdbx/models"
)

/*
	Begin - создание и старт новой транзакции MVCC поверх транзакций FDB.

	В первой транзакции мы создаем новый объект, после чего записываем его в новый случайный ключ.
	В момент записи FDB автоматически добавит в значение версию, которую и будем использовать для
	дальнейшей идентификации и кеширования статусов после фиксации.

	Вторая транзакция нужна, чтобы прочитать это значение, удалить его и записать объект как положено.

	Такая несколько схема кажется извращением, но она призвана одновременно достичь следующих целей:
	* Получить монотонно возрастающий идентификатор транзакции, синхронизированный между всеми инстансами
	* Не создать ни одного конфликта записи/чтения между параллельно стартующими транзакциями
*/
func Begin(conn db.Connection) (_ Tx, exp error) {
	t := &tx64{
		conn:   conn,
		start:  uint64(time.Now().UTC().UnixNano()),
		status: txStatusRunning,
	}

	uid := uuid.New()

	if exp = conn.Write(func(w db.Writer) (err error) {
		if err = w.SetVersion(nsTxTmp, uid[:]); err != nil {
			return ErrSaveTx.WithReason(err)
		}

		return nil
	}); exp != nil {
		return nil, ErrBegin.WithReason(exp)
	}

	if exp = conn.Write(func(w db.Writer) (err error) {
		var ver *db.Pair

		if ver, err = w.Pair(nsTxTmp, uid[:]); err != nil {
			return ErrFetchTx.WithReason(err).WithDebug(errors.Debug{
				"uid": uid.String(),
			})
		}

		t.txid = binary.BigEndian.Uint64(ver.Value[:8])

		if err = w.SetPair(nsTx, ver.Value[:8], t.pack()); err != nil {
			return ErrSaveTx.WithReason(err).WithDebug(errors.Debug{
				"txid": t.txid,
				"txbk": t.txbk(t.txid),
			})
		}

		if err = w.DropPair(nsTxTmp, uid[:]); err != nil {
			return ErrDropTx.WithReason(err).WithDebug(errors.Debug{
				"uid": uid.String(),
			})
		}

		return nil
	}); exp != nil {
		return nil, ErrBegin.WithReason(exp)
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

/*
	Select - выборка актуального в данной транзакции значения ключа.
*/
func (t *tx64) Select(key Key) (res Value, err error) {
	// Получаем номер текущей операции
	// В случае конфликтов и повторов НЕ будет увеличиваться, и это правильно
	opid := atomic.AddUint32(&t.opid, 1)

	if err = t.conn.Read(func(r db.Reader) (exp error) {
		var pair *db.Pair

		// Локальный кеш статусов транзакций, чтобы не лазать 100500 раз по незавершенным
		// При конфликте и повторах инициализируется каждый раз заново, и это правильно!
		lc := newStatusCache()

		if pair, exp = t.fetchRow(r, lc, opid, key); exp != nil {
			return
		}

		if pair != nil {
			res = &rowValue{models.GetRootAsRow(pair.Value, 0)}
		}

		return nil
	}); err != nil {
		return nil, ErrSelect.WithReason(err)
	}

	return res, nil
}

/*
	Upsert - вставка или обновление актуальной в данный момент записи.

	Чтобы найти актуальную запись, нужно сделать по сути обычный Select.
	Если актуальной записи не существует, можно просто добавить новую.
	Обновить - значит удалить актуальное значение и добавить новое.

	Важно, чтобы выборка и обновление шли строго в одной внутренней FDB транзакции.
*/
func (t *tx64) Upsert(key Key, value Value) (exp error) {
	// Получаем номер текущей операции
	// В случае конфликтов и повторов НЕ будет увеличиваться, и это правильно
	opid := atomic.AddUint32(&t.opid, 1)

	if exp = t.conn.Write(func(w db.Writer) (err error) {
		var pair *db.Pair

		// Локальный кеш статусов транзакций, чтобы не лазать 100500 раз по незавершенным
		// При конфликте и повторах инициализируется каждый раз заново, и это правильно!
		lc := newStatusCache()

		if pair, err = t.fetchRow(w, lc, opid, key); err != nil {
			return
		}

		if err = t.dropRow(w, opid, pair); err != nil {
			return
		}

		if err = t.saveRow(w, opid, key.Bytes(), value.Bytes()); err != nil {
			return
		}

		return nil
	}); exp != nil {
		return ErrUpsert.WithReason(exp)
	}

	return nil
}

/*
	Delete - удаление актуальной в данный момент записи, если она существует.

	Чтобы найти актуальную запись, нужно сделать по сути обычный Select.
	Удалить - значит обновить значение в служебных полях и записать в тот же ключ.

	Важно, чтобы выборка и обновление шли строго в одной внутренней FDB транзакции.
*/
func (t *tx64) Delete(key Key) (exp error) {
	// Получаем номер текущей операции
	// В случае конфликтов и повторов НЕ будет увеличиваться, и это правильно
	opid := atomic.AddUint32(&t.opid, 1)

	if exp = t.conn.Write(func(w db.Writer) (err error) {
		var pair *db.Pair

		// Локальный кеш статусов транзакций, чтобы не лазать 100500 раз по незавершенным
		// При конфликте и повторах инициализируется каждый раз заново, и это правильно!
		lc := newStatusCache()

		if pair, err = t.fetchRow(w, lc, opid, key); err != nil {
			return
		}

		if err = t.dropRow(w, opid, pair); err != nil {
			return
		}

		return nil
	}); exp != nil {
		return ErrDelete.WithReason(exp)
	}

	return nil
}

func (t *tx64) Commit() (err error) {

	if err = t.close(txStatusCommitted); err != nil {
		return ErrCommit.WithReason(err)
	}

	txCache.set(t.txid, txStatusCommitted)
	return nil
}

func (t *tx64) Cancel() (err error) {

	if err = t.close(txStatusAborted); err != nil {
		return ErrCancel.WithReason(err)
	}

	txCache.set(t.txid, txStatusAborted)
	return nil
}

func (t *tx64) isCommitted(local *statusCache, r db.Reader, x uint64) (ok bool, err error) {
	var status byte

	// Дешевле всего чекнуть в глобальном кеше, вдруг уже знаем такую
	if status = txCache.get(x); status != txStatusUnknown {
		return status == txStatusCommitted, nil
	}

	// Возможно, это открытая транзакция из локального кеша
	if status = local.get(x); status != txStatusUnknown {
		return status == txStatusCommitted, nil
	}

	var txPair *db.Pair
	txbk := t.txbk(x)

	// Придется слазать в БД за статусом и положить в кеш
	if txPair, err = r.Pair(nsTx, txbk); err != nil {
		return false, ErrFetchTx.WithReason(err).WithDebug(errors.Debug{
			"txid": x,
			"txbk": txbk,
		})
	}

	// Получаем значение статуса как часть модели
	status = models.GetRootAsTransaction(txPair.Value, 0).Status()

	// В случае финальных статусов можем положить в глобальный кеш
	if status == txStatusCommitted || status == txStatusAborted {
		txCache.set(x, status)
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
	buf := fbs.NewBuilder(32)
	buf.Finish(tx.Pack(buf))
	return buf.FinishedBytes()
}

func (t *tx64) packRow(opid uint32, value []byte) []byte {
	row := &models.RowT{
		State: &models.RowStateT{
			XMin: t.txid,
			CMin: opid,
		},
		Data: value,
	}
	buf := fbs.NewBuilder(32)
	buf.Finish(row.Pack(buf))
	return buf.FinishedBytes()
}

func (t *tx64) close(status byte) (err error) {
	if t.status == txStatusCommitted || t.status == txStatusAborted {
		return nil
	}

	t.status = status
	dump := t.pack()
	txbk := t.txbk(t.txid)

	if err = t.conn.Write(func(w db.Writer) (exp error) {
		if exp = w.SetPair(nsTx, txbk, dump); exp != nil {
			return ErrSaveTx.WithReason(err).WithDebug(errors.Debug{
				"txid": t.txid,
				"txbk": txbk,
			})
		}

		return nil
	}); err != nil {
		return ErrCloseTx.WithReason(err)
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
func (t *tx64) fetchRow(r db.Reader, lc *statusCache, opid uint32, key Key) (res *db.Pair, err error) {
	var comm bool
	var list []*db.Pair

	// Загружаем список версий, существующих в рамках этой операции
	if list, err = r.List(nsUser, key.Bytes(), 0, true); err != nil {
		return nil, ErrFetchRow.WithReason(err).WithDebug(errors.Debug{
			"key": fmt.Sprintf("%x", key.Bytes()),
		})
	}

	glog.Errorf("txid = %d", t.txid)
	glog.Errorf("opid = %d", opid)
	glog.Errorf("list = %+v", list)
	defer func() {
		glog.Errorf("res = %p", res)
		glog.Flush()
	}()

	// Проверяем все версии, пока не получим актуальную
	for i := range list {
		row := models.GetRootAsRow(list[i].Value, 0).State(nil).UnPack()

		glog.Errorf("=-=- %d -=-=", i)
		glog.Errorf("xmin = %d", row.XMin)
		glog.Errorf("xmax = %d", row.XMax)
		glog.Errorf("cmin = %d", row.CMin)
		glog.Errorf("cmax = %d", row.CMax)

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
			return nil, ErrFetchRow.WithReason(err)
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
			if comm, err = t.isCommitted(lc, r, row.XMax); err != nil {
				return nil, ErrFetchRow.WithReason(err)
			}

			// Если запись была удалена другой транзакцией, но она еще закоммичена, то еще видна
			if !comm {
				return list[i], nil
			}
		}
	}

	return nil, nil
}

func (t *tx64) dropRow(w db.Writer, opid uint32, pair *db.Pair) (err error) {
	if pair == nil {
		return nil
	}

	glog.Errorf("value = %s", pair.Value)

	state := models.GetRootAsRow(pair.Value, 0).State(nil)

	if !state.MutateXMax(t.txid) {
		return ErrDropRow.WithStack()
	}

	if !state.MutateCMax(opid) {
		return ErrDropRow.WithStack()
	}

	if err = w.SetPair(nsUser, pair.Key, pair.Value); err != nil {
		return ErrDropRow.WithReason(err).WithDebug(errors.Debug{
			"key": fmt.Sprintf("%x", pair.Key),
		})
	}

	return nil
}

func (t *tx64) saveRow(w db.Writer, opid uint32, key []byte, val []byte) (err error) {

	if err = w.SetPair(nsUser, append(key, t.txbk(t.txid)...), t.packRow(opid, val)); err != nil {
		return ErrSaveRow.WithReason(err).WithDebug(errors.Debug{
			"key": fmt.Sprintf("%x", key),
			"len": len(val),
		})
	}

	return nil
}

func (t *tx64) txbk(x uint64) []byte {
	var txid [8]byte
	binary.BigEndian.PutUint64(txid[:], x)
	return txid[:]
}
