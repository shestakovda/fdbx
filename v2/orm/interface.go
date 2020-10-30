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
	Cursor(mvcc.Tx, string) (Query, error)
	Upsert(mvcc.Tx, ...fdbx.Pair) error
	Delete(mvcc.Tx, ...fdbx.Pair) error

	Autovacuum(context.Context, db.Connection, ...Option)
}

// Queue - универсальный интерфейс очередей, для работы с задачами
type Queue interface {
	ID() uint16

	Ack(mvcc.Tx, ...fdbx.Key) (err error)
	Pub(mvcc.Tx, fdbx.Key, ...Option) error
	PubList(mvcc.Tx, []fdbx.Key, ...Option) error

	Sub(context.Context, db.Connection, int) (<-chan Task, <-chan error)
	SubList(context.Context, db.Connection, int) ([]Task, error)

	Stat(mvcc.Tx) (int64, int64, error)
	Lost(mvcc.Tx, int) ([]Task, error)
	Task(mvcc.Tx, fdbx.Key) (Task, error)
}

// Task - исполняемый элемент очереди
type Task interface {
	Key() fdbx.Key
	Body() []byte
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
	ByID(ids ...fdbx.Key) Query
	PossibleByID(ids ...fdbx.Key) Query
	ByIndex(idx uint16, query fdbx.Key) Query

	Reverse() Query
	Limit(int) Query
	Where(Filter) Query

	All() ([]fdbx.Pair, error)
	First() (fdbx.Pair, error)
	Delete() error

	Agg(...Aggregator) error

	Drop() error
	Save() (string, error)
	Sequence(context.Context) (<-chan fdbx.Pair, <-chan error)
}

// Selector - поставщик сырых данных для запроса
type Selector interface {
	LastKey() fdbx.Key
	Select(context.Context, Table, ...Option) (<-chan fdbx.Pair, <-chan error)
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
	ErrFirst     = errx.New("Ошибка загрузки первого значения")
	ErrSequence  = errx.New("Ошибка загрузки коллекции")
	ErrLoadQuery = errx.New("Ошибка загрузки курсора запроса")
	ErrDropQuery = errx.New("Ошибка удаления курсора запроса")
	ErrSaveQuery = errx.New("Ошибка сохранения курсора запроса")
)
