package orm

import (
	"context"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
)

// Task status
const (
	StatusPublished   byte = 1
	StatusUnconfirmed byte = 2
	StatusConfirmed   byte = 3
)

// Table - универсальный интерфейс коллекции, чтобы работать с запросами
type Table interface {
	ID() uint16
	Mgr() fdbx.KeyManager

	Select(mvcc.Tx) Query
	Upsert(mvcc.Tx, ...fdbx.Pair) error
	Delete(mvcc.Tx, ...fdbx.Pair) error
}

// Queue - универсальный интерфейс очередей, для работы с задачами
type Queue interface {
	ID() uint16

	Ack(mvcc.Tx, ...fdbx.Key) (err error)
	Pub(mvcc.Tx, time.Time, ...fdbx.Key) error

	Sub(context.Context, db.Connection, int) (<-chan fdbx.Pair, <-chan error)
	SubList(context.Context, db.Connection, int) ([]fdbx.Pair, error)

	Stat(mvcc.Tx) (int64, int64, error)
	Lost(mvcc.Tx, int) ([]fdbx.Pair, error)
	Status(mvcc.Tx, ...fdbx.Key) (map[string]byte, error)
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
	Select(Table) ([]fdbx.Pair, error)
}

// Filter - управляющий объект для фильтрации выборок
type Filter interface {
	Skip(fdbx.Pair) (bool, error)
}

// IndexKey - для получения ключа при индексации коллекций
type IndexKey func([]byte) fdbx.Key

// Option - доп.аргумент для инициализации коллекций
type Option func(*options)

// Ошибки модуля
var (
	ErrSub       = errx.New("Ошибка получения задач из очереди")
	ErrPub       = errx.New("Ошибка публикации задачи в очередь")
	ErrAck       = errx.New("Ошибка подтверждения задач в очереди")
	ErrLost      = errx.New("Ошибка получения неподтвержденных задач")
	ErrStat      = errx.New("Ошибка получения статистики задач")
	ErrStatus    = errx.New("Ошибка получения статуса задач")
	ErrWatch     = errx.New("Ошибка отслеживания результата задачи")
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
