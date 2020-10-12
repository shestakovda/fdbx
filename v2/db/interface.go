package db

import (
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
)

// ListGetter - метод для отложенного получения списка значений
type ListGetter func() []fdbx.Pair

// Reader - обработчик чтения значений из БД
type Reader interface {
	Data(fdbx.Key) (fdbx.Pair, error)
	List(from, to fdbx.Key, limit uint64, reverse bool) (ListGetter, error)
}

// Reader - обработчик модификации значений в БД
type Writer interface {
	Reader

	Upsert(fdbx.Pair) error
	Delete(fdbx.Key) error

	Versioned(fdbx.Key) error
	Increment(fdbx.Key, uint64) error
}

// Connection - объект подключения к БД, а также фабрика элементов
type Connection interface {
	// Получение номера БД, для контроля или формирования ключей
	DB() byte

	// ОПАСНО! Единовременная и полная очистка всех данных в БД
	// Рекомендуется использовать только в unit-тестах!
	Clear() error

	// Основные функции транзакций
	Read(func(Reader) error) error
	Write(func(Writer) error) error
}

// ConnectV610 - создание нового подключения к серверу FDB и базе данных.
//
// Идентификатор базы всего 1 байт, потому что пока не рассчитываем на то, что разных БД будет так много.
// Особое значение 0xFF (255) запрещено, т.к. с этого байта начинается служебная область видимости FDB.
//
// Если указан путь к файлу, то подключается к нему. Иначе идет по стандартному (зависит от ОС).
//
// Этот драйвер настроен на совместимость с конкретной версией клиента, с другими может не заработать.
func ConnectV610(databaseID byte, opts ...Option) (Connection, error) {
	return newConnV610(databaseID, opts...)
}

// Ошибки модуля
var (
	ErrRead    = errx.New("Ошибка транзакции чтения")
	ErrWrite   = errx.New("Ошибка транзакции записи")
	ErrClear   = errx.New("Ошибка транзакции очистки")
	ErrConnect = errx.New("Ошибка подключения к FoundationDB")
)
