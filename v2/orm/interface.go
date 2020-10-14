package orm

import (
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

// Collection - универсальный интерфейс коллекции, чтобы работать с запросами
type Collection interface {
	ID() uint16

	Select(mvcc.Tx) Query
	Upsert(mvcc.Tx, ...fdbx.Pair) error
	Delete(mvcc.Tx, ...fdbx.Pair) error
}

// TaskCollection - универсальный интерфейс очередей, для работы с задачами
type TaskCollection interface {
	ID() byte

	Pub(mvcc.Tx, time.Time, ...fdbx.Key) error
}

// AggFunc - описание функции-агрегатора для запросов
type AggFunc func(fdbx.Pair) error

// Query - универсальный интерфейс объекта запроса данных, основная логика
type Query interface {
	ByID(ids ...fdbx.Key) Query
	PossibleByID(ids ...fdbx.Key) Query

	All() ([]fdbx.Pair, error)
	First() (fdbx.Pair, error)
	Delete() error

	Agg(...AggFunc) error
}

// Selector - поставщик сырых данных для запроса
type Selector interface {
	Select(Collection) ([]fdbx.Pair, error)
}

// Filter - управляющий объект для фильтрации выборок
type Filter interface {
	Skip(fdbx.Pair) (bool, error)
}

// IndexKey - для получения ключа при индексации коллекций
type IndexKey func(fdbx.Value) fdbx.Key

// Option - доп.аргумент для инициализации коллекций
type Option func(*options)

// Ошибки модуля
var (
	ErrSub       = errx.New("Ошибка получения задач из очереди")
	ErrPub       = errx.New("Ошибка публикации задачи в очередь")
	ErrAgg       = errx.New("Ошибка агрегации объектов коллекции")
	ErrSelect    = errx.New("Ошибка загрузки объектов коллекции")
	ErrDelete    = errx.New("Ошибка удаления объектов коллекции")
	ErrUpsert    = errx.New("Ошибка обновления объектов коллекции")
	ErrNotFound  = errx.New("Ошибка загрузки объекта")
	ErrIdxDelete = errx.New("Ошибка очистки индекса")
	ErrIdxUpsert = errx.New("Ошибка обновления индекса")
	ErrValPack   = errx.New("Ошибка упаковки значения")
	ErrValUnpack = errx.New("Ошибка распаковки значения")
)
