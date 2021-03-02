package mvcc

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
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
	Begin - создание и старт новой транзакции MVCC поверх транзакций FDB.

	В первой транзакции мы создаем новый объект, после чего записываем его в новый случайный ключ.
	В момент записи FDB автоматически добавит в значение версию, которую и будем использовать для
	дальнейшей идентификации и кеширования статусов после фиксации.

	Вторая транзакция нужна, чтобы прочитать это значение, удалить его и записать объект как положено.

	Такая несколько схема кажется извращением, но она призвана одновременно достичь следующих целей:
	* Получить монотонно возрастающий идентификатор транзакции, синхронизированный между всеми инстансами
	* Не создать ни одного конфликта записи/чтения между параллельно стартующими транзакциями
*/
func newTx64(conn db.Connection) *tx64 {
	lctx, exit := context.WithCancel(context.Background())
	return &tx64{
		conn:   conn,
		txid:   newTxID(),
		status: txStatusRunning,
		start:  time.Now().UTC().UnixNano(),
		wait:   new(sync.WaitGroup),
		lctx:   lctx,
		exit:   exit,
	}
}

/*
tx64 - объект "логической" транзакции MVCC поверх "физической" транзакции FDB
*/
type tx64 struct {
	sync.RWMutex

	// Read-only
	txid  suid
	conn  db.Connection
	start int64

	// Atomic
	opid uint32
	mods uint32

	// RWMutex
	status byte
	oncomm []CommitHandler
	locks  map[string]fdb.Key

	// Lock update management
	wait *sync.WaitGroup
	lctx context.Context
	exit context.CancelFunc
}

func (t *tx64) Conn() db.Connection {
	return t.conn
}

/*
	OnCommit - Регистрация хука для выполнения при удачном завершении транзакции
*/
func (t *tx64) OnCommit(hdl CommitHandler) {
	t.Lock()
	defer t.Unlock()
	t.oncomm = append(t.oncomm, hdl)
}

/*
	Commit - Успешное завершение (принятие) транзакции
	Перед завершением выполняет хуки OnCommit
	Поддерживает опции Writer
*/
func (t *tx64) Commit(args ...Option) (err error) {
	opts := getOpts(args)

	if err = t.onCommit(opts.writer); err != nil {
		return ErrClose.WithReason(err)
	}

	return t.close(opts.writer, txStatusCommitted)
}

// applyOnCommit - применяет все зарегистрированные на коммит хуки
func (t *tx64) onCommit(w db.Writer) (err error) {
	t.RLock()
	t.RUnlock()

	if len(t.oncomm) == 0 {
		return nil
	}

	return t.applyWriteHandler(w, t.applyOnCommit, true)
}

// Применяет все установленные в процессе транзакции патчи на коммит
func (t *tx64) applyOnCommit(w db.Writer) (err error) {
	for i := range t.oncomm {
		if err = t.oncomm[i](w); err != nil {
			return
		}
	}

	return nil
}

// Применяет обработчик в указанный Writer или выполняет его в новой физ.транзакции
func (t *tx64) applyWriteHandler(w db.Writer, h db.WriteHandler, mod bool) error {
	if mod {
		atomic.AddUint32(&t.mods, 1)
	}

	if w.Empty() {
		return t.conn.Write(h)
	}

	return h(w)
}

// Cancel - Неудачное завершение (отклонение) транзакции
// Поддерживает опции Writer
func (t *tx64) Cancel(args ...Option) {
	if err := t.close(getOpts(args).writer, txStatusCancelled); err != nil {
		glog.Errorf("Ошибка завершения транзакции %+v", err)
	}
}

/*
	Delete - удаление актуальной в данный момент записи, если она существует.
	Чтобы найти актуальную запись, нужно сделать по сути обычный Select.
	Удалить - значит обновить значение в служебных полях и записать в тот же ключ.
	Важно, чтобы выборка и обновление шли строго в одной внутренней FDB транзакции.
*/
func (t *tx64) Delete(keys []fdb.Key, args ...Option) (err error) {
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

	if err = t.applyWriteHandler(opts.writer, hdlr, true); err != nil {
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

	if err = t.applyWriteHandler(opts.writer, hdlr, true); err != nil {
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
			w.Lock(ukey, ukey)
		}

		if exp = read(w.Reader); exp != nil {
			return
		}

		if opts.onLock != nil {
			return opts.onLock(t, w, res)
		}

		return nil
	}

	if opts.lock {
		err = t.applyWriteHandler(opts.writer, hdlr, opts.onLock != nil)
	} else {
		err = t.conn.Read(read)
	}

	if err != nil {
		return fdb.KeyValue{}, ErrSelect.WithReason(err)
	}

	return res, nil
}

/*
	Select - выборка актуального в данной транзакции значения ключа.
*/
func (t *tx64) SelectMany(keys []fdb.Key, args ...Option) (res map[string]fdb.KeyValue, err error) {
	opts := getOpts(args)
	opid := atomic.AddUint32(&t.opid, 1)
	ukeys := make([]fdb.Key, len(keys))
	for i := range ukeys {
		ukeys[i] = WrapKey(keys[i])
	}
	res = make(map[string]fdb.KeyValue, len(keys))

	read := func(r db.Reader) (exp error) {
		var rows []fdb.KeyValue

		lc := makeCache()
		lgs := make([]fdb.RangeResult, len(ukeys))

		// prefetch
		for i := range ukeys {
			lgs[i] = r.List(ukeys[i], ukeys[i], 0, true, false)
		}

		for i := range ukeys {
			if rows, exp = t.fetchRows(r, lc, opid, lgs[i], false, len(ukeys[i])); exp != nil {
				return
			}

			if len(rows) == 0 {
				continue
			}

			if len(rows) > 1 {
				return ErrDuplicate.WithDebug(errx.Debug{"key": ukeys[i]})
			}

			res[keys[i].String()] = usrPair(rows[0])
		}

		return nil
	}
	hdlr := func(w db.Writer) (exp error) {
		if opts.lock {
			for i := range ukeys {
				w.Lock(ukeys[i], ukeys[i])
			}
		}

		if exp = read(w.Reader); exp != nil {
			return
		}

		if opts.onLock != nil {
			for _, kv := range res {
				if exp = opts.onLock(t, w, kv); exp != nil {
					return
				}
			}
		}

		return nil
	}

	if opts.lock {
		err = t.applyWriteHandler(opts.writer, hdlr, opts.onLock != nil)
	} else {
		err = t.conn.Read(read)
	}

	if err != nil {
		return nil, ErrSelect.WithReason(err)
	}

	return res, nil
}

/*
	ListAll - Последовательная выборка всех активных ключей в диапазоне
	Поддерживает опции From, To, Reverse, Limit, PackSize, Exclusive, Writer
*/
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

func (t *tx64) Count(prefix fdb.Key) (cnt int64, err error) {
	const max = 1 << 16

	prefix = WrapKey(prefix)

	if cnt, err = t.getFastCount(prefix); err != nil {
		return
	}

	if cnt > 0 {
		return cnt, nil
	}

	cnt = 0
	cpu := runtime.NumCPU()
	wg := new(sync.WaitGroup)
	wg.Add(cpu)

	rate := 5
	irate := 1 << rate
	count := max / irate
	list := make([]fdb.RangeResult, count)
	plen := len(prefix)
	from := make([]byte, plen+2)
	last := make([]byte, plen+2)
	copy(from[:plen], prefix)
	copy(last[:plen], prefix)

	errs := make(chan error, cpu)
	parts := make(chan []fdb.KeyValue, count)
	cache := makeCache()
	opid := atomic.AddUint32(&t.opid, 1)

	for i := 0; i < cpu; i++ {
		go func() {
			var ok bool
			var exp error
			defer wg.Done()

			r := db.Reader{}
			tmp := int64(0)

			for part := range parts {
				for k := range part {
					if ok, exp = t.isVisible(r, cache, opid, part[k], false); exp != nil {
						errs <- ErrSeqScan.WithReason(exp)
						return
					}

					if ok {
						tmp++
					}
				}
			}

			atomic.AddInt64(&cnt, tmp)
		}()
	}

	if err = t.conn.Read(func(r db.Reader) (exp error) {
		defer close(parts)

		r = r.Snapshot()

		for i := 0; i < count; i++ {
			from[plen] = byte(i * irate >> 8)
			from[plen+1] = byte(i * irate)
			if i < count-1 {
				last[plen] = byte(((i+1)*irate - 1) >> 8)
				last[plen+1] = byte((i+1)*irate - 1)
			} else {
				last[plen] = 0xFF
				last[plen+1] = 0xFF
			}
			list[i] = r.List(from, last, 0, false, false)
		}

		for i := range list {
			if part := list[i].GetSliceOrPanic(); len(part) > 0 {
				parts <- part
			}
		}

		return nil
	}); err != nil {
		return 0, ErrSeqScan.WithReason(err)
	}

	wg.Wait()
	close(errs)

	for err = range errs {
		if err != nil {
			return
		}
	}

	return cnt, nil
}

func (t *tx64) getFastCount(prefix fdb.Key) (cnt int64, err error) {
	const fastLimit = 1000
	cache := makeCache()
	opid := atomic.AddUint32(&t.opid, 1)

	if err = t.conn.Read(func(r db.Reader) (exp error) {
		var ok bool

		r = r.Snapshot()

		for _, item := range r.List(prefix, prefix, fastLimit, false, false).GetSliceOrPanic() {
			if ok, exp = t.isVisible(r, cache, opid, item, false); exp != nil {
				return
			}

			if ok {
				cnt++
			}
		}
		return nil
	}); err != nil {
		return
	}

	// Достигли предела? Значит объектов больше, придется пересчитать еще раз и точнее
	if cnt == fastLimit {
		cnt = 0
	}

	return cnt, nil
}

/*
	SeqScan - Последовательная выборка всех активных ключей в диапазоне
	Поддерживает опции From, To, Reverse, Limit, PackSize, Exclusive, Writer
*/
func (t *tx64) SeqScan(ctx context.Context, args ...Option) (<-chan fdb.KeyValue, <-chan error) {
	list := make(chan fdb.KeyValue)
	errs := make(chan error, 1)

	go func() {
		var err error
		var part []fdb.KeyValue

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

		if opts.limit > 0 && (opts.spack > uint64(10*opts.limit)) {
			opts.spack = uint64(10 * opts.limit)
		}

		for {
			if ctx.Err() != nil {
				return
			}

			if err = t.applyWriteHandler(opts.writer, hdlr, opts.onLock != nil); err != nil {
				errs <- ErrSeqScan.WithReason(err)
				return
			}

			skip = true

			if len(part) == 0 {
				if rows < int(opts.spack) || (opts.spack == 0 && rows == 0) {
					return
				}
				continue
			}

			for i := range part {
				select {
				case list <- part[i]:
					if size++; opts.limit > 0 && size >= opts.limit {
						return
					}
				case <-ctx.Done():
					return
				}
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
) (rows int, part []fdb.KeyValue, last fdb.Key, err error) {
	var ok bool

	// Блокировка не означает модификаций
	if opts.lock {
		w.Lock(from, to)
	}

	wctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	iter := w.List(from, to, opts.spack, opts.reverse, skip).Iterator()
	part = make([]fdb.KeyValue, 0, 2048)

	// Может оказаться, что данных слишком много или сервер перегружен, тогда мы можем не успеть
	// загрузить объекты вовремя, т.е. нам не хватит времени на обработку. В этом случае сделаем размер пачки меньше
	// и выйдем, чтобы можно было загружать меньшими кусками
	for iter.Advance() && wctx.Err() == nil {
		item := iter.MustGet()
		item.Key = item.Key[1:]

		if ok, err = t.isVisible(w.Reader, lc, opid, item, false); err != nil {
			// Скорее всего кончилась транзакция, в следующей пачке получим
			return rows, part, last, nil
		}

		last = item.Key
		rows++

		if ok {
			item = usrPair(item)

			if opts.onLock != nil {
				if err = opts.onLock(t, w, item); err != nil {
					return
				}
			}

			if part = append(part, item); opts.limit > 0 && size+len(part) >= opts.limit {
				break
			}
		}
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

/*
	SaveBLOB - Сохранение больших бинарных данных по ключу
*/
func (t *tx64) SaveBLOB(key fdb.Key, blob []byte, args ...Option) (err error) {
	opts := getOpts(args)

	sum := 0
	tmp := blob
	num := uint16(0)
	prs := make([]fdb.KeyValue, 0, opts.rowmem/opts.rowsize+1)
	set := func(w db.Writer) error { w.Upsert(prs...); return nil }

	// Разбиваем на элементарные значения, пишем пачками по 10 мб
	for {
		if len(tmp) > opts.rowsize {
			prs = append(prs, fdb.KeyValue{
				Key:   fdbx.AppendRight(WrapKey(key), byte(num>>8), byte(num)),
				Value: tmp[:opts.rowsize],
			})
			tmp = tmp[opts.rowsize:]
			sum += opts.rowsize
			num++
		} else {
			prs = append(prs, fdb.KeyValue{
				Key:   fdbx.AppendRight(WrapKey(key), byte(num>>8), byte(num)),
				Value: tmp,
			})
			sum += len(tmp)
			num++
			break
		}

		if sum >= opts.rowmem-opts.rowsize {
			if err = t.applyWriteHandler(opts.writer, set, true); err != nil {
				return ErrBLOBSave.WithReason(err)
			}
			sum = 0
			prs = prs[:0]
		}
	}

	if len(prs) > 0 {
		if err = t.applyWriteHandler(opts.writer, set, true); err != nil {
			return ErrBLOBSave.WithReason(err)
		}
	}

	return nil
}

/*
	LoadBLOB - Загрузка бинарных данных по ключу, указывается ожидаемый размер
*/
func (t *tx64) LoadBLOB(key fdb.Key, _ ...Option) (_ []byte, err error) {
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

/*
	DropBLOB - Удаление бинарных данных по ключу
	Поддерживает опции Writer
*/
func (t *tx64) DropBLOB(key fdb.Key, args ...Option) (err error) {
	opts := getOpts(args)
	ukey := WrapKey(key)
	hdlr := func(w db.Writer) error { w.Erase(ukey, ukey); return nil }

	if err = t.applyWriteHandler(opts.writer, hdlr, true); err != nil {
		return ErrBLOBDrop.WithReason(err)
	}

	return nil
}

/*
	SharedLock - Блокировка записи с доступом на чтение по сигнальному ключу
*/
func (t *tx64) SharedLock(keys ...fdb.Key) (err error) {
	var lock db.Waiter
	var exist bool

	usrKeys := make([]fdb.Key, len(keys))
	for i := range keys {
		usrKeys[i] = WrapLockKey(keys[i])
	}

	// Сначала проверим, можем ли мы вообще получить такую блокировку
	if exist, err = t.checkLock(usrKeys); err != nil {
		return
	}

	// Блокировка уже получена - никаких проблем
	if exist {
		return nil
	}

	// Стараемся получить блокировку, если занято - ожидаем
	cnt := 0
	for {
		ack := false
		start := time.Now()

		// Попытка поставить блокировку
		if err = t.conn.Write(func(w db.Writer) (exp error) {
			var val []byte

			now := fdbx.Time2Byte(time.Now())
			pairs := make([]fdb.KeyValue, len(usrKeys))
			items := make([]fdb.FutureByteSlice, len(usrKeys))

			for i, ukey := range usrKeys {
				w.Lock(ukey, ukey)
				pairs[i] = fdb.KeyValue{Key: ukey, Value: now}
				items[i] = w.Item(ukey)
			}

			for i := range usrKeys {
				// Если есть значение ключа, значит блокировка занята, придется ждать
				if val = items[i].MustGet(); len(val) > 0 {
					var upd time.Time

					// Если блокировка давно не обновлялась - значит ей кранты, забираем себе
					if upd, exp = fdbx.Byte2Time(val); exp != nil {
						return
					}

					if time.Since(upd) < 30*time.Second {
						lock = w.Watch(usrKeys[i])
						return nil
					}
				}
			}

			// Если нам все-таки удается поставить значение - значит блокировка наша
			w.Upsert(pairs...)
			ack = true
			return nil
		}); err != nil {
			return ErrSharedLock.WithReason(err)
		}

		query := time.Since(start)
		start = time.Now()

		// Блокировка наша, можно ехать дальше
		// Значение может быть true тогда и только тогда, когда удалось завершить метод без ошибок
		if ack {
			t.appendLock(usrKeys)
			glog.Errorf("Получили блокировку: попыток %d, запрос %s", cnt, query)
			return nil
		}

		// Значение уже стоит, ждем освобождения
		if lock != nil {
			func() {
				wctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = lock.Resolve(wctx)
			}()
		}

		wait := time.Since(start)

		glog.Errorf("Итерация блокировки %d: запрос %s, ожидание %s", cnt, query, wait)
		cnt++
	}
}

func (t *tx64) ReleaseLocks() {
	if !t.waitLocks() {
		return
	}

	t.Lock()
	defer t.Unlock()
	var w db.Writer

	if err := t.applyWriteHandler(w, t.onRelease, true); err != nil {
		glog.Errorf("%+v", ErrReleaseLock.WithReason(err))
	}

	t.lctx, t.exit = context.WithCancel(context.Background())
	t.locks = nil
}

func (t *tx64) waitLocks() bool {
	t.RLock()
	defer t.RUnlock()

	if len(t.locks) == 0 {
		return false
	}

	// Останавливаем все обновления
	t.exit()
	t.wait.Wait()
	return true
}

func (t *tx64) isCommitted(local *txCache, r db.Reader, txid suid) (_ bool, err error) {
	var status byte

	if status, err = t.txStatus(local, r, txid); err != nil {
		return
	}

	return status == txStatusCommitted, nil
}

func (t *tx64) isCancelled(local *txCache, r db.Reader, txid suid) (_ bool, err error) {
	var status byte

	if status, err = t.txStatus(local, r, txid); err != nil {
		return
	}

	return status == txStatusCancelled, nil
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
	var val []byte

	hdlr := func(r2 db.Reader) error {
		val = r2.Data(WrapTxKey(txid[:]))
		return nil
	}

	if r.Empty() {
		err = t.conn.Read(hdlr)
	} else {
		err = hdlr(r)
	}
	if err != nil {
		return
	}

	// Если в БД нет записи, то либо транзакция еще открыта, либо это был откат
	// Поскольку нет определенности, в глобальный кеш ничего не складываем
	if len(val) == 0 {
		local.set(txid, txStatusRunning)
		return txStatusRunning, nil
	}

	// Получаем значение статуса как часть модели
	status = models.GetRootAsTransaction(val, 0).Status()

	// В случае финальных статусов можем положить в глобальный кеш
	if status == txStatusCommitted || status == txStatusCancelled {
		globCache.set(txid, status)
	}

	// В локальный кеш можем положить в любом случае, затем вернуть
	local.set(txid, status)
	return status, nil
}

/*
	close - закрывает транзакцию и применяет или откатывает изменения.

	Интерпретация транзакций в БД идет следующим образом:

	* Если запись есть в БД и статус "закоммичено" - значит закоммичено
	* Если запись есть в БД и статус "в процессе" - значит еще в процессе
	* Если запись есть в БД и статус "отменено" - значит отменено
	* Если записи нет в БД - то транзакция считается отмененной (aborted)
*/
func (t *tx64) close(w db.Writer, status byte) (err error) {
	// Останавливаем все блокировки
	t.ReleaseLocks()

	t.Lock()
	defer t.Unlock()

	// Если статус транзакции уже определен, менять его нельзя
	if t.status == txStatusCommitted || t.status == txStatusCancelled {
		return nil
	}
	t.status = status

	// Если в рамках транзакции не было никаких изменений (флаг mods), то обходимся только установкой кеша
	// Это оптимизация транзакций на чтение, поскольку они должны быть максимально "бесплатны" для юзера
	if atomic.LoadUint32(&t.mods) == 0 {
		globCache.set(t.txid, t.status)
		return nil
	}

	// Cохраняем в БД объект с обновленным статусом
	if err = t.applyWriteHandler(w, t.save, true); err != nil {
		return ErrClose.WithReason(err)
	}

	// При удачном стечении обстоятельств - устанавливаем глобальный кеш
	globCache.set(t.txid, t.status)
	return nil
}

// Cохраняем в БД объект с текущим статусом
func (t *tx64) save(w db.Writer) error {
	w.Upsert(fdb.KeyValue{
		Key: WrapTxKey(t.txid[:]),
		Value: fdbx.FlatPack(&models.TransactionT{
			Start:  t.start,
			Status: t.status,
		}),
	})
	return nil
}

func (t *tx64) rowTxData(key fdb.Key) (xmin suid, cmin uint32) {
	kidx := len(key) - 16
	copy(xmin[:8], key[kidx:kidx+8])
	copy(xmin[8:12], key[kidx+12:kidx+16])
	return xmin, binary.BigEndian.Uint32(key[kidx+8 : kidx+12])
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
			glog.Errorf("panic mvcc.Tx.isVisible(%s): %+v", item.Key, rec)
			ok = false
			err = errx.ErrInternal.WithDebug(errx.Debug{
				"key":   item.Key,
				"panic": rec,
			})
		}
	}()

	xmin, cmin := t.rowTxData(item.Key)

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
		if fail, err = t.isCancelled(lc, r, xmin); err != nil {
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

	// Распаковываем строку
	mod := models.GetRootAsRow(item.Value, 0)

	// В этом случае объект еще не был удален, значит виден
	if mod.DropLength() == 0 {
		return true, nil
	}
	row := mod.UnPack()

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

	for _, pair := range pairs {

		if buf := pair.Value; len(buf) > 0 {
			row = models.GetRootAsRow(buf, 0).UnPack()

			// Обработчик имеет возможность предотвратить удаление/обновление, выбросив ошибку
			if onDelete != nil {
				if err = onDelete(t, w, fdb.KeyValue{Key: UnwrapKey(pair.Key), Value: row.Data}); err != nil {
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
			w.Upsert(fdb.KeyValue{Key: pair.Key, Value: fdbx.FlatPack(row)})
		}
	}
	return nil
}

/*
	Touch - Изменение сигнального ключа, чтобы сработали Watch
	По сути, выставляет хук OnCommit с правильным содержимым
*/
func (t *tx64) Touch(key fdb.Key) {
	t.OnCommit(func(w db.Writer) error {
		w.Upsert(fdb.KeyValue{Key: WrapWatchKey(key), Value: fdbx.Time2Byte(time.Now())})
		return nil
	})
}

/*
Watch - Ожидание изменения сигнального ключа в Touch
*/
func (t *tx64) Watch(key fdb.Key) (wait db.Waiter, err error) {
	return wait, t.conn.Write(func(w db.Writer) error {
		wait = w.Watch(WrapWatchKey(key))
		return nil
	})
}

/*
Vacuum - Запуск очистки устаревших записей ключей по указанному префиксу
*/
func (t *tx64) Vacuum(prefix fdb.Key, args ...Option) (err error) {
	skip := false
	opts := getOpts(args)
	from := WrapKey(prefix)
	last := WrapKey(prefix)
	hdlr := func(w db.Writer) (exp error) {
		lg := w.List(from, last, opts.vpack, false, skip)

		if from, exp = t.vacuumPart(w, lg, opts.onVacuum); exp != nil {
			return
		}

		return nil
	}

	for {
		if err = t.applyWriteHandler(opts.writer, hdlr, true); err != nil {
			return ErrVacuum.WithReason(err)
		}

		// Пустой ключ - значит больше не было строк, условие выхода
		if len(from) == 0 {
			return nil
		}
		skip = true

		// Передышка, чтобы не слишком грузить бд
		time.Sleep(time.Second)
	}
}

// vacuumPart - функция обратная fetchRows, в том смысле, что она удаляет все ключи, которые больше не нужны в БД
func (t *tx64) vacuumPart(w db.Writer, lg fdb.RangeResult, onVacuum RowHandler) (last fdb.Key, err error) {
	var ok bool

	lc := makeCache()
	rows := 0
	iter := lg.Iterator()
	opid := atomic.AddUint32(&t.opid, 1)
	wctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

func (t *tx64) checkLock(keys []fdb.Key) (exist bool, err error) {
	t.RLock()
	defer t.RUnlock()

	// Если никаких блокировок еще не существует, можем блокировать что угодно
	if len(t.locks) == 0 {
		return false, nil
	}

	// Если хотя бы один ключ не был заблокирован ранее - новую блокировку получать нельзя
	for i := range keys {
		if len(t.locks[keys[i].String()]) == 0 {
			return false, ErrAlreadyLocked.WithDebug(errx.Debug{
				"locks": t.locks,
			})
		}
	}

	// Похоже, что все ключи повторно блокируются, никаких проблем
	return true, nil
}

func (t *tx64) appendLock(keys []fdb.Key) {
	t.Lock()
	defer t.Unlock()

	t.locks = make(map[string]fdb.Key, len(keys))
	for i := range keys {
		t.locks[keys[i].String()] = keys[i]
	}

	t.wait.Add(1)
	go t.updateLocks()
}

func (t *tx64) updateLocks() {
	defer t.wait.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := t.conn.Write(func(w db.Writer) error {
				t.RLock()
				defer t.RUnlock()
				if len(t.locks) == 0 {
					t.exit()
					return nil
				}
				for _, key := range t.locks {
					w.Upsert(fdb.KeyValue{Key: key, Value: fdbx.Time2Byte(time.Now())})
				}
				return nil
			}); err != nil {
				// Ни в коем случае нельзя продолжать работу, если блокировка накрылась
				panic(fmt.Errorf("ошибка обновления блокировки: %+v", err))
			}
		case <-t.lctx.Done():
			return
		}
	}
}

func (t *tx64) onRelease(w db.Writer) error {
	for _, lk := range t.locks {
		w.Delete(lk)
	}
	return nil
}
