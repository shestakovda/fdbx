package db

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
)

// ConnectV610 - создание нового подключения к серверу FDB и базе данных.
//
// Идентификатор базы всего 1 байт, потому что пока не рассчитываем на то, что разных БД будет так много.
// Особое значение 0xFF (255) запрещено, т.к. с этого байта начинается служебная область видимости FDB.
//
// Если указан путь к файлу, то подключается к нему. Иначе идет по стандартному (зависит от ОС).
//
// Этот драйвер настроен на совместимость с конкретной версией клиента, с другими может не заработать.
func ConnectV610(dbID byte, opts ...Option) (Connection, error) {
	return newConnV610(dbID, opts...)
}

// ReadHandler - обработчик физической транзакции чтения, должен быть идемпотентным
type ReadHandler func(Reader) error

// WriteHandler - обработчик физической транзакции записи, должен быть идемпотентным
type WriteHandler func(Writer) error

// Connection - объект подключения к БД, а также фабрика элементов
type Connection interface {
	// Получение номера БД, для контроля или формирования ключей
	DB() byte

	// ОПАСНО! Единовременная и полная очистка всех данных в БД
	// Рекомендуется использовать только в unit-тестах!
	Clear() error

	// Основные функции транзакций
	Read(ReadHandler) error
	Write(WriteHandler) error
}

// Reader - обработчик чтения значений из БД (физическая транзакция)
type Reader interface {
	// Получение одного значения по конкретному ключу
	Data(fdbx.Key) fdbx.Pair

	// Получение списка значений в интервале [from, to)
	// В случае реверса, интервал [to, from)
	List(from, to fdbx.Key, limit uint64, reverse bool) fdbx.ListGetter
}

// Writer - обработчик модификации значений в БД (физическая транзакция)
type Writer interface {
	Reader

	// Version - получение полной версии транзакции на запись
	Version() fdb.FutureKey

	// Удаление конкретного значения. Не расстраивается, если его нет
	Delete(fdbx.Key)

	// Вставка или обновление значения по ключу
	Upsert(...fdbx.Pair)

	// Атомарный инкремент (или декремент) LittleEndian-значения по ключу
	Increment(fdbx.Key, int64)

	// Эксклюзивная блокировка интервала
	Lock(fdbx.Key, fdbx.Key)

	// Очистка всех данных интервала
	Erase(fdbx.Key, fdbx.Key)

	// Старт отслеживания изменения значения конкретного ключа
	Watch(fdbx.Key) fdbx.Waiter
}

// Ошибки модуля
var (
	ErrWait    = errx.New("Ошибка ожидания значения")
	ErrRead    = errx.New("Ошибка транзакции чтения")
	ErrWrite   = errx.New("Ошибка транзакции записи")
	ErrClear   = errx.New("Ошибка транзакции очистки")
	ErrConnect = errx.New("Ошибка подключения к FoundationDB")
)
