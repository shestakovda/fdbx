package orm

import (
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx"
	"github.com/shestakovda/fdbx/mvcc"
)

// Collection - универсальный интерфейс коллекции, чтобы работать с запросами
type Collection interface {
	ID() uint16

	Select(mvcc.Tx) Query
	Upsert(mvcc.Tx, ...fdbx.Pair) error
	Delete(mvcc.Tx, ...fdbx.Pair) error
}

// AggFunc - описание функции-агрегатора для запросов
type AggFunc func(fdbx.Pair) error

// Query - универсальный интерфейс объекта запроса данных, основная логика
type Query interface {
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
	ErrAgg       = errx.New("Ошибка агрегации объектов коллекции")
	ErrSelect    = errx.New("Ошибка загрузки объектов коллекции")
	ErrDelete    = errx.New("Ошибка удаления объектов коллекции")
	ErrUpsert    = errx.New("Ошибка обновления объектов коллекции")
	ErrIdxDelete = errx.New("Ошибка очистки индекса")
	ErrIdxUpsert = errx.New("Ошибка обновления индекса")
)
