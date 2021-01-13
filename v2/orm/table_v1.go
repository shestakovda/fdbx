package orm

import (
	"context"
	"encoding/binary"
	"math/rand"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/golang/glog"
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/models"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

func NewTable(id uint16, args ...Option) Table {
	return &v1Table{
		id:      id,
		options: getOpts(args),
	}
}

type v1Table struct {
	options
	id uint16
}

func (t *v1Table) ID() uint16 { return t.id }

func (t *v1Table) Select(tx mvcc.Tx) Query { return NewQuery(t, tx) }

func (t *v1Table) Cursor(tx mvcc.Tx, id string) (Query, error) { return loadQuery(t, tx, id) }

func (t *v1Table) Insert(tx mvcc.Tx, pairs ...fdb.KeyValue) (err error) {
	return t.upsert(tx, true, pairs...)
}

func (t *v1Table) Upsert(tx mvcc.Tx, pairs ...fdb.KeyValue) (err error) {
	return t.upsert(tx, false, pairs...)
}

func (t *v1Table) Delete(tx mvcc.Tx, keys ...fdb.Key) (err error) {
	if len(keys) == 0 {
		return nil
	}

	cp := make([]fdb.Key, len(keys))
	for i := range keys {
		cp[i] = WrapTableKey(t.id, keys[i])
	}

	if err = tx.Delete(cp, mvcc.OnDelete(t.onDelete)); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (t *v1Table) upsert(tx mvcc.Tx, unique bool, pairs ...fdb.KeyValue) (err error) {
	if len(pairs) == 0 {
		return nil
	}

	cp := make([]fdb.KeyValue, len(pairs))
	for i := range pairs {
		if cp[i], err = newSysPair(tx, t.id, pairs[i]); err != nil {
			return ErrUpsert.WithReason(err)
		}
	}

	opts := []mvcc.Option{
		mvcc.OnInsert(t.onInsert),
		mvcc.OnDelete(t.onDelete),
	}

	if unique {
		opts = append(opts, mvcc.OnUpdate(t.onUpdate))
	}

	if err = tx.Upsert(cp, opts...); err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

func (t *v1Table) onInsert(tx mvcc.Tx, w db.Writer, pair fdb.KeyValue) (err error) {
	if len(t.options.batchidx) == 0 {
		return nil
	}

	var usr fdb.KeyValue

	// Здесь нам придется обернуть еще значение, которое возвращается, потому что оно не обработано уровнем ниже
	if usr, err = newUsrPair(tx, t.id, pair); err != nil {
		return ErrIdxUpsert.WithReason(err)
	}

	pval := usr.Value
	pkey := usr.Key
	rows := make([]fdb.KeyValue, 0, 32)
	var dict map[uint16][]fdb.Key

	for k := range t.options.batchidx {
		if dict, err = t.options.batchidx[k](pval); err != nil {
			return
		}

		if len(dict) == 0 {
			continue
		}

		for idx, keys := range dict {
			for i := range keys {
				if len(keys[i]) == 0 {
					continue
				}
				rows = append(rows, fdb.KeyValue{fdbx.AppendRight(WrapIndexKey(t.id, idx, keys[i]), pkey...), pkey})
			}
		}
	}

	if err = tx.Upsert(rows); err != nil {
		return ErrIdxUpsert.WithReason(err)
	}

	return nil
}

func (t *v1Table) onUpdate(tx mvcc.Tx, w db.Writer, pair fdb.KeyValue) (err error) {
	return ErrDuplicate.WithDebug(errx.Debug{
		"key": pair.Key,
	})
}

func (t *v1Table) onDelete(tx mvcc.Tx, w db.Writer, pair fdb.KeyValue) (err error) {
	var usr fdb.KeyValue

	if len(t.options.batchidx) == 0 {
		return nil
	}

	// Здесь нам придется обернуть еще значение, которое возвращается, потому что оно не обработано уровнем ниже
	if usr, err = newUsrPair(tx, t.id, pair); err != nil {
		return ErrIdxDelete.WithReason(err)
	}

	uval := usr.Value
	rows := make([]fdb.Key, 0, 32)
	pkey := UnwrapTableKey(pair.Key)
	var dict map[uint16][]fdb.Key

	for k := range t.options.batchidx {
		if dict, err = t.options.batchidx[k](uval); err != nil {
			return
		}

		if len(dict) == 0 {
			continue
		}

		for idx, keys := range dict {
			for i := range keys {
				if len(keys[i]) == 0 {
					continue
				}
				rows = append(rows, fdbx.AppendRight(WrapIndexKey(t.id, idx, keys[i]), pkey...))
			}
		}
	}

	if err = tx.Delete(rows); err != nil {
		return ErrIdxDelete.WithReason(err)
	}

	return nil
}

func (t *v1Table) Autovacuum(ctx context.Context, cn db.Connection, args ...Option) {
	var err error
	var tbid [2]byte
	var timer *time.Timer

	binary.BigEndian.PutUint16(tbid[:], t.id)
	tkey := fdb.Key(tbid[:]).String()
	opts := getOpts(args)

	defer func() {
		// Перезапуск только в случае ошибки
		if err != nil {
			time.Sleep(time.Second)

			// И только если мы вообще можем еще запускать
			if ctx.Err() == nil {
				// Тогда стартуем заново и в s.wait ничего не ставим
				go t.Autovacuum(ctx, cn)
				return
			}
		}
	}()

	// Отлавливаем панику и превращаем в ошибку
	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(error); ok {
				err = ErrVacuum.WithReason(e)
			} else {
				err = ErrVacuum.WithDebug(errx.Debug{"panic": rec})
			}
		}
	}()

	// Работать должен постоянно
	glog.Errorf("Start autovacuum on %s", tkey)
	for ctx.Err() == nil {

		// Выбираем случайное время с 00:00 до 06:00
		// Чтобы делать темные делишки под покровом ночи
		if opts.vwait > 0 {
			timer = time.NewTimer(opts.vwait)
		} else {
			now := time.Now()
			min := rand.Intn(60)
			hour := rand.Intn(7)
			when := time.Date(now.Year(), now.Month(), now.Day()+1, hour, min, 00, 00, now.Location())
			timer = time.NewTimer(when.Sub(now))
		}

		select {
		case <-timer.C:
			glog.Errorf("Run vacuum on %s", tkey)

			if err = t.Vacuum(cn); err != nil {
				return
			}

			glog.Errorf("Complete vacuum on %s", tkey)
		case <-ctx.Done():
			return
		}
	}
}

func (t *v1Table) Vacuum(dbc db.Connection) error {
	if exp := mvcc.WithTx(dbc, func(tx mvcc.Tx) (err error) {

		// Этот запрос очищает только данные. Для них должен быть обработчик очистки BLOB
		if err = tx.Vacuum(WrapTableKey(t.id, nil), mvcc.OnVacuum(t.onVacuum)); err != nil {
			return
		}

		// Отдельно очистка всех индексов
		if err = tx.Vacuum(fdbx.SkipRight(WrapIndexKey(t.id, 0, nil), 2)); err != nil {
			return
		}

		// Отдельно очистка всех очередей
		if err = tx.Vacuum(fdbx.SkipRight(WrapQueueKey(t.id, 0, nil, 0, nil), 3)); err != nil {
			return
		}

		// Отдельно очистка всех курсоров
		if err = tx.Vacuum(WrapQueryKey(t.id, nil)); err != nil {
			return
		}

		return nil
	}); exp != nil {
		return ErrVacuum.WithReason(exp)
	}
	return nil
}

func (t *v1Table) onVacuum(tx mvcc.Tx, w db.Writer, p fdb.KeyValue) (err error) {
	var mod models.ValueT

	val := p.Value

	if len(val) == 0 {
		return nil
	}

	models.GetRootAsValue(val, 0).UnPackTo(&mod)

	// Если значение лежит в BLOB, надо удалить
	if mod.Blob {
		if err = tx.DropBLOB(WrapBlobKey(t.id, mod.Data), mvcc.Writer(w)); err != nil {
			return ErrVacuum.WithReason(err)
		}
	}

	return nil
}
