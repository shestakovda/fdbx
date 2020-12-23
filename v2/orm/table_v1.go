package orm

import (
	"context"
	"time"

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

func (t *v1Table) Insert(tx mvcc.Tx, pairs ...fdbx.Pair) (err error) {
	return t.upsert(tx, true, pairs...)
}

func (t *v1Table) Upsert(tx mvcc.Tx, pairs ...fdbx.Pair) (err error) {
	return t.upsert(tx, false, pairs...)
}

func (t *v1Table) Delete(tx mvcc.Tx, keys ...fdbx.Key) (err error) {
	if len(keys) == 0 {
		return nil
	}

	cp := make([]fdbx.Key, len(keys))
	for i := range keys {
		cp[i] = WrapTableKey(t.id, keys[i])
	}

	if err = tx.Delete(cp, mvcc.OnDelete(t.onDelete)); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (t *v1Table) upsert(tx mvcc.Tx, ins bool, pairs ...fdbx.Pair) (err error) {
	if len(pairs) == 0 {
		return nil
	}

	cp := make([]fdbx.Pair, len(pairs))
	for i := range pairs {
		if cp[i], err = newSysPair(tx, t.id, pairs[i]); err != nil {
			return ErrUpsert.WithReason(err)
		}
	}

	if err = tx.Upsert(cp, mvcc.OnUpdate(t.onUpdate(ins)), mvcc.OnDelete(t.onDelete)); err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

func (t *v1Table) onUpdate(unique bool) mvcc.UpdateHandler {
	return func(tx mvcc.Tx, pair fdbx.Pair, upd bool) (err error) {
		if unique && upd {
			return ErrDuplicate.WithDebug(errx.Debug{
				"key": pair.Key().Printable(),
			})
		}

		if len(t.options.batchidx) == 0 {
			return nil
		}

		pair = pair.Unwrap()
		pval := pair.Value()
		pkey := pair.Key().Bytes()
		rows := make([]fdbx.Pair, 0, 32)
		var dict map[uint16][]fdbx.Key

		for k := range t.options.batchidx {
			if dict, err = t.options.batchidx[k](pval); err != nil {
				return
			}

			if len(dict) == 0 {
				continue
			}

			for idx, keys := range dict {
				for i := range keys {
					if keys[i] == nil || len(keys[i].Bytes()) == 0 {
						continue
					}
					rows = append(rows, fdbx.NewPair(WrapIndexKey(t.id, idx, keys[i]).RPart(pkey...), pkey))
				}
			}
		}

		if err = tx.Upsert(rows); err != nil {
			return ErrIdxUpsert.WithReason(err)
		}

		return nil
	}
}

func (t *v1Table) onDelete(tx mvcc.Tx, pair fdbx.Pair) (err error) {
	var usr fdbx.Pair

	if len(t.options.batchidx) == 0 {
		return nil
	}

	// Здесь нам придется обернуть еще значение, которое возвращается, потому что оно не обработано уровнем ниже
	if usr, err = newUsrPair(tx, t.id, pair); err != nil {
		return ErrIdxDelete.WithReason(err)
	}

	uval := usr.Value()
	rows := make([]fdbx.Key, 0, 32)
	pkey := UnwrapTableKey(pair.Key()).Bytes()
	var dict map[uint16][]fdbx.Key

	for k := range t.options.batchidx {
		if dict, err = t.options.batchidx[k](uval); err != nil {
			return
		}

		if len(dict) == 0 {
			continue
		}

		for idx, keys := range dict {
			for i := range keys {
				if keys[i] == nil || len(keys[i].Bytes()) == 0 {
					continue
				}
				rows = append(rows, WrapIndexKey(t.id, idx, keys[i]).RPart(pkey...))
			}
		}
	}

	if err = tx.Delete(rows); err != nil {
		return ErrIdxDelete.WithReason(err)
	}

	return nil
}

func (t *v1Table) Autovacuum(ctx context.Context, cn db.Connection) {
	var err error

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
	glog.Errorf("Start autovacuum on \\x%d\\x%d", byte(t.id>>8), byte(t.id))
	timer := time.NewTimer(time.Second)
	for ctx.Err() == nil {
		// Очистка таймера
		if !timer.Stop() {
			<-timer.C
		}

		// Выбираем случайное время с 00:00 до 06:00
		// Чтобы делать темные делишки под покровом ночи
		// now := time.Now()
		// min := rand.Intn(60)
		// hour := rand.Intn(7)
		// when := time.Date(now.Year(), now.Month(), now.Day()+1, hour, min, 00, 00, now.Location())
		// timer.Reset(when.Sub(now))
		timer.Reset(5 * time.Minute)

		select {
		case <-timer.C:
			glog.Errorf("Run vacuum on \\x%d\\x%d", byte(t.id>>8), byte(t.id))

			if err = t.Vacuum(cn); err != nil {
				return
			}

			glog.Errorf("Complete vacuum on \\x%d\\x%d", byte(t.id>>8), byte(t.id))
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
		if err = tx.Vacuum(WrapIndexKey(t.id, 0, nil).RSkip(2)); err != nil {
			return
		}

		// Отдельно очистка всех очередей
		if err = tx.Vacuum(WrapQueueKey(t.id, 0, nil, 0, nil).RSkip(3)); err != nil {
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

func (t *v1Table) onVacuum(tx mvcc.Tx, p fdbx.Pair, w db.Writer) (err error) {
	var mod models.ValueT

	val := p.Value()

	if len(val) == 0 {
		return nil
	}

	models.GetRootAsValue(val, 0).UnPackTo(&mod)

	// Если значение лежит в BLOB, надо удалить
	if mod.Blob {
		if err = tx.DropBLOB(WrapBlobKey(t.id, fdbx.Bytes2Key(mod.Data)), mvcc.Writer(w)); err != nil {
			return ErrVacuum.WithReason(err)
		}
	}

	return nil
}
