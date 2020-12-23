package orm

import (
	"context"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

// Debug - флаг отладочных принтов
var Debug = false

// Task status
const (
	StatusPublished   byte = 1
	StatusUnconfirmed byte = 2
	StatusConfirmed   byte = 3
)

// Table - универсальный интерфейс коллекции, чтобы работать с запросами
type Table interface {
	ID() uint16

	Select(mvcc.Tx) Query
	Cursor(mvcc.Tx, string) (Query, error)
	Delete(mvcc.Tx, ...fdbx.Key) error
	Upsert(mvcc.Tx, ...fdbx.Pair) error
	Insert(mvcc.Tx, ...fdbx.Pair) error

	Vacuum(db.Connection) error
	Autovacuum(context.Context, db.Connection)
}

// Queue - универсальный интерфейс очередей, для работы с задачами
type Queue interface {
	ID() uint16

	Ack(mvcc.Tx, ...fdbx.Key) (err error)
	Pub(mvcc.Tx, fdbx.Key, ...Option) error
	PubList(mvcc.Tx, []fdbx.Key, ...Option) error

	Sub(context.Context, db.Connection, int) (<-chan Task, <-chan error)
	SubList(context.Context, db.Connection, int) ([]Task, error)

	Undo(mvcc.Tx, fdbx.Key) error
	Stat(mvcc.Tx) (int64, int64, error)
	Lost(mvcc.Tx, int) ([]Task, error)
	Task(mvcc.Tx, fdbx.Key) (Task, error)
}

// Task - исполняемый элемент очереди
type Task interface {
	Key() fdbx.Key
	Body() []byte
	Pair() fdbx.Pair
	Status() byte
	Repeats() uint32
	Creator() string
	Created() time.Time
	Planned() time.Time
	Headers() map[string]string

	Ack(mvcc.Tx) error
	Repeat(mvcc.Tx, time.Duration) error
}

// Aggregator - описание функции-агрегатора для запросов
type Aggregator func(fdbx.Pair) error

// Filter - управляющий метод для фильтрации выборок
// Должен возвращать true, если объект нужно оставить и false в другом случае
type Filter func(fdbx.Pair) (ok bool, err error)

// Query - универсальный интерфейс объекта запроса данных, основная логика
type Query interface {
	// Критерии выбора (селекторы)
	ByID(ids ...fdbx.Key) Query
	PossibleByID(ids ...fdbx.Key) Query
	ByIndex(idx uint16, query fdbx.Key) Query
	ByIndexRange(idx uint16, from, last fdbx.Key) Query
	BySelector(Selector) Query

	// Модификаторы селекторов
	Reverse() Query
	Page(int) Query
	Limit(int) Query
	Where(Filter) Query

	// Обработка результатов
	Agg(...Aggregator) error
	All() ([]fdbx.Pair, error)
	Next() ([]fdbx.Pair, error)
	First() (fdbx.Pair, error)
	Sequence(context.Context) (<-chan fdbx.Pair, <-chan error)
	Delete() error
	Empty() bool

	// Сохранение запроса (курсор)
	Save() (string, error)
	Drop() error
}

// Selector - поставщик сырых данных для запроса
type Selector interface {
	Select(context.Context, Table, ...Option) (<-chan fdbx.Pair, <-chan error)
}

// IndexKey - для получения ключей при индексации коллекций
type IndexKey func([]byte) (fdbx.Key, error)

// IndexMultiKey - для получения ключей при индексации коллекций
type IndexMultiKey func([]byte) ([]fdbx.Key, error)

// IndexBatchKey - для получения ключей при индексации коллекций
type IndexBatchKey func([]byte) (map[uint16][]fdbx.Key, error)

// Option - доп.аргумент для инициализации коллекций
type Option func(*options)

// WrapTableKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapTableKey(tbid uint16, key fdbx.Key) fdbx.Key {
	if key == nil {
		key = fdbx.Bytes2Key(nil)
	}
	return key.LPart(byte(tbid>>8), byte(tbid), nsData)
}

// UnwrapTableKey - обертка ключа для получения пользовательского ключа из системного, при загрузке
func UnwrapTableKey(key fdbx.Key) fdbx.Key {
	if key != nil {
		return key.LSkip(3)
	}
	return nil
}

// WrapBlobKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapBlobKey(tbid uint16, key fdbx.Key) fdbx.Key {
	if key == nil {
		key = fdbx.Bytes2Key(nil)
	}
	return key.LPart(byte(tbid>>8), byte(tbid), nsBLOB)
}

// UnwrapBlobKey - обертка ключа для получения пользовательского ключа из системного, при загрузке
func UnwrapBlobKey(key fdbx.Key) fdbx.Key {
	if key != nil {
		return key.LSkip(3)
	}
	return nil
}

// WrapIndexKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapIndexKey(tbid, idxid uint16, key fdbx.Key) fdbx.Key {
	if key == nil {
		key = fdbx.Bytes2Key(nil)
	}
	return key.LPart(byte(tbid>>8), byte(tbid), nsIndex, byte(idxid>>8), byte(idxid))
}

// UnwrapIndexKey - обертка ключа для получения пользовательского ключа из системного, при загрузке
func UnwrapIndexKey(key fdbx.Key) fdbx.Key {
	if key != nil {
		return key.LSkip(5)
	}
	return nil
}

// WrapQueueKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapQueueKey(tbid, qid uint16, pref []byte, flag byte, key fdbx.Key) fdbx.Key {
	if key == nil {
		key = fdbx.Bytes2Key(nil)
	}

	key = key.LPart(flag)

	if len(pref) > 0 {
		key = key.LPart(pref...)
	}

	return key.LPart(byte(tbid>>8), byte(tbid), nsQueue, byte(qid>>8), byte(qid))
}

// UnwrapQueueKey - обертка ключа для получения пользовательского ключа из системного, при загрузке
func UnwrapQueueKey(pref []byte, key fdbx.Key) fdbx.Key {
	if key != nil {
		// основные байты (5) + префикс + флаг (1) + метка времени (8)
		return key.LSkip(5 + uint16(len(pref)) + 1 + 8)
	}
	return nil
}

// WrapQueryKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapQueryKey(tbid uint16, key fdbx.Key) fdbx.Key {
	if key == nil {
		key = fdbx.Bytes2Key(nil)
	}
	return key.LPart(byte(tbid>>8), byte(tbid), nsQuery)
}

// UnwrapQueryKey - обертка ключа для получения пользовательского ключа из системного, при загрузке
func UnwrapQueryKey(key fdbx.Key) fdbx.Key {
	if key != nil {
		return key.LSkip(3)
	}
	return nil
}

// Ошибки модуля
var (
	ErrSub       = errx.New("Ошибка получения задач из очереди")
	ErrPub       = errx.New("Ошибка публикации задачи в очередь")
	ErrAck       = errx.New("Ошибка подтверждения задач в очереди")
	ErrUndo      = errx.New("Ошибка отмены опубликованной задачи")
	ErrLost      = errx.New("Ошибка получения неподтвержденных задач")
	ErrStat      = errx.New("Ошибка получения статистики задач")
	ErrTask      = errx.New("Ошибка получения метаданных задачи")
	ErrWatch     = errx.New("Ошибка отслеживания результата задачи")
	ErrAgg       = errx.New("Ошибка агрегации объектов коллекции")
	ErrSelect    = errx.New("Ошибка загрузки объектов коллекции")
	ErrDelete    = errx.New("Ошибка удаления объектов коллекции")
	ErrUpsert    = errx.New("Ошибка обновления объектов коллекции")
	ErrFilter    = errx.New("Ошибка фильтрации объектов коллекции")
	ErrNotFound  = errx.New("Ошибка загрузки объекта")
	ErrIdxDelete = errx.New("Ошибка очистки индекса")
	ErrIdxUpsert = errx.New("Ошибка обновления индекса")
	ErrValPack   = errx.New("Ошибка упаковки значения")
	ErrValUnpack = errx.New("Ошибка распаковки значения")
	ErrVacuum    = errx.New("Ошибка автоочистки значений")
	ErrAll       = errx.New("Ошибка загрузки всех значений")
	ErrNext      = errx.New("Ошибка загрузки страницы значений")
	ErrFirst     = errx.New("Ошибка загрузки первого значения")
	ErrSequence  = errx.New("Ошибка загрузки коллекции")
	ErrLoadQuery = errx.New("Ошибка загрузки курсора запроса")
	ErrDropQuery = errx.New("Ошибка удаления курсора запроса")
	ErrSaveQuery = errx.New("Ошибка сохранения курсора запроса")
	ErrDuplicate = errx.New("Нарушение уникальности коллекции")
)
