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
	t := v1Table{
		id:      id,
		mgr:     newTableKeyManager(id),
		options: getOpts(args),
	}

	t.idx = make(map[uint16]fdbx.KeyManager, len(t.options.indexes))
	for idx := range t.options.indexes {
		t.idx[idx] = newIndexKeyManager(id, idx)
	}

	return &t
}

type v1Table struct {
	options
	id  uint16
	mgr fdbx.KeyManager
	idx map[uint16]fdbx.KeyManager
}

func (t v1Table) ID() uint16 { return t.id }

func (t v1Table) Mgr() fdbx.KeyManager { return t.mgr }

func (t v1Table) Select(tx mvcc.Tx) Query { return NewQuery(&t, tx) }

func (t v1Table) Cursor(tx mvcc.Tx, id string) (Query, error) { return loadQuery(&t, tx, id) }

func (t v1Table) Upsert(tx mvcc.Tx, pairs ...fdbx.Pair) (err error) {
	if len(pairs) == 0 {
		return nil
	}

	valWrapper := usrValWrapper(tx, t.id)

	cp := make([]fdbx.Pair, len(pairs))
	for i := range pairs {
		// Для гарантии того, что мы "не тронем" исходные данные, делаем копию
		cp[i] = pairs[i].Clone()

		// Для того, чтобы избавиться от врапперов и получить целевое значение, применяем их
		if err = cp[i].Apply(); err != nil {
			ErrUpsert.WithReason(err)
		}

		// Оборачиваем местными врапперами работы с ключами и BLOB
		cp[i] = cp[i].WrapKey(t.mgr.Wrapper).WrapValue(valWrapper)
	}

	if err = tx.Upsert(cp, mvcc.OnInsert(t.onInsert), mvcc.OnDelete(t.onUpdate)); err != nil {
		return ErrUpsert.WithReason(err)
	}

	return nil
}

func (t v1Table) Delete(tx mvcc.Tx, pairs ...fdbx.Pair) (err error) {
	if len(pairs) == 0 {
		return nil
	}

	cp := make([]fdbx.Key, len(pairs))
	for i := range pairs {
		// Сначала получаем исходный ключ, чтобы не задеть исходные данные
		if cp[i], err = pairs[i].Key(); err != nil {
			return ErrDelete.WithReason(err)
		}

		// Оборачиваем ключ
		cp[i] = t.mgr.Wrap(cp[i])
	}

	if err = tx.Delete(cp, mvcc.OnDelete(t.onDelete)); err != nil {
		return ErrDelete.WithReason(err)
	}

	return nil
}

func (t v1Table) onInsert(tx mvcc.Tx, pair fdbx.Pair) (err error) {
	if len(t.options.indexes) == 0 {
		return nil
	}

	var ups [1]fdbx.Pair
	var key, tmp fdbx.Key

	// Здесь берем сырое значение по двум причинам:
	// 1. Вызов Upsert гарантирует Apply, т.е. в Raw будет исходное значение, даже если выше по стеку были врапперы
	// 2. Нельзя использовать Value, т.к. врапперы работы с blob будут дублировать значения
	raw := pair.Raw()

	for idx, fnc := range t.options.indexes {
		if tmp = fnc(raw); tmp == nil {
			continue
		}

		if key, err = pair.Key(); err != nil {
			return ErrIdxUpsert.WithReason(err)
		}

		ups[0] = fdbx.NewPair(t.idx[idx].Wrap(tmp), t.mgr.Unwrap(key))
		if err = tx.Upsert(ups[:]); err != nil {
			return ErrIdxUpsert.WithReason(err).WithDebug(errx.Debug{"idx": idx})
		}
	}

	return nil

}

func (t v1Table) onUpdate(tx mvcc.Tx, pair fdbx.Pair) (err error) {
	if len(t.options.indexes) == 0 {
		return nil
	}

	var ups [1]fdbx.Key

	// Здесь берем сырое значение по двум причинам:
	// 1. Вызов Upsert гарантируют Apply, т.е. в Raw будет исходное значение, даже если выше по стеку были врапперы
	// 2. Нельзя использовать Value, т.к. врапперы работы с blob будут дублировать значения
	raw := pair.Raw()

	for idx, fnc := range t.options.indexes {
		if ups[0] = fnc(raw); ups[0] == nil {
			continue
		}

		ups[0] = t.idx[idx].Wrap(ups[0])

		if err = tx.Delete(ups[:]); err != nil {
			return ErrIdxDelete.WithReason(err).WithDebug(errx.Debug{"idx": idx})
		}
	}

	return nil
}

func (t v1Table) onDelete(tx mvcc.Tx, pair fdbx.Pair) (err error) {
	if len(t.options.indexes) == 0 {
		return nil
	}

	var raw []byte
	var ups [1]fdbx.Key

	// Здесь нам придется обернуть еще значение, которое возвращается, потому что оно не обработано уровнем ниже
	if raw, err = pair.WrapValue(sysValWrapper(tx, t.id)).Value(); err != nil {
		return ErrIdxDelete.WithReason(err)
	}

	for idx, fnc := range t.options.indexes {
		if ups[0] = fnc(raw); ups[0] == nil {
			continue
		}

		ups[0] = t.idx[idx].Wrap(ups[0])

		if err = tx.Delete(ups[:]); err != nil {
			return ErrIdxDelete.WithReason(err).WithDebug(errx.Debug{"idx": idx})
		}
	}

	return nil
}

func (t v1Table) Autovacuum(ctx context.Context, cn db.Connection, args ...Option) {
	var err error

	opts := getOpts(args)
	tick := time.NewTicker(opts.vwait)
	defer tick.Stop()

	defer func() {
		// Перезапуск только в случае ошибки
		if err != nil {
			glog.Errorf("%+v", err)
			time.Sleep(time.Second)

			// И только если мы вообще можем еще запускать
			if ctx.Err() == nil {
				// Тогда стартуем заново и в s.wait ничего не ставим
				go t.Autovacuum(ctx, cn, args...)
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

	for ctx.Err() == nil {

		if err = t.vacuumStep(cn); err != nil {
			return
		}

		select {
		case <-tick.C:
		case <-ctx.Done():
			return
		}
	}
}

func (t v1Table) vacuumStep(cn db.Connection) (err error) {
	var tx mvcc.Tx

	if tx, err = mvcc.Begin(cn); err != nil {
		return ErrVacuum.WithReason(err)
	}
	defer tx.Cancel()

	// Этот запрос очищает только данные. Для них должен быть обработчик очистки BLOB
	if err = tx.Vacuum(t.mgr.Wrap(nil), mvcc.OnVacuum(t.onVacuum)); err != nil {
		return ErrVacuum.WithReason(err)
	}

	// Отдельно очистка всех индексов
	if err = tx.Vacuum(newIndexKeyManager(t.id, 0).Wrap(nil).RSkip(2)); err != nil {
		return ErrVacuum.WithReason(err)
	}

	// Отдельно очистка всех очередей
	if err = tx.Vacuum(newQueueKeyManager(t.id, 0, nil).Wrap(nil).RSkip(2)); err != nil {
		return ErrVacuum.WithReason(err)
	}

	// Отдельно очистка всех блобов
	if err = tx.Vacuum(newBLOBKeyManager(t.id).Wrap(nil)); err != nil {
		return ErrVacuum.WithReason(err)
	}

	// Отдельно очистка всех курсоров
	if err = tx.Vacuum(NewQueryKeyManager(t.id).Wrap(nil)); err != nil {
		return ErrVacuum.WithReason(err)
	}

	return nil
}

func (t v1Table) onVacuum(tx mvcc.Tx, p fdbx.Pair, w db.Writer) (err error) {
	var val []byte
	var mod models.ValueT

	if val, err = p.Value(); err != nil {
		return ErrVacuum.WithReason(err)
	}

	if len(val) == 0 {
		return nil
	}

	models.GetRootAsValue(val, 0).UnPackTo(&mod)

	// Если значение лежит в BLOB, надо удалить
	if mod.Blob {
		if err = tx.DropBLOB(newBLOBKeyManager(t.id).Wrap(fdbx.Key(mod.Data)), mvcc.Writer(w)); err != nil {
			return ErrVacuum.WithReason(err)
		}
	}

	return nil
}
