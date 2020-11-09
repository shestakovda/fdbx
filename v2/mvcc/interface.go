package mvcc

import (
	"context"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
)

// TxCacheSize - размер глобального кеша статусов завершенных транзакций
var TxCacheSize = 8000000

// ReadOnly - создание и старт новой транзакции только для чтения
func ReadOnly(conn db.Connection) (Tx, error) { return newTx64(conn, true) }

// ReadTx - объект "логической" транзакции MVCC поверх "физической" транзакции FDB
// Только для чтения. Не требует коммита или роллбэка, т.к. не может внести изменений
type ReadTx interface {
	// Ссылка на подключение к БД, на всякий случай
	Conn() db.Connection

	// Выборка актуального значения для ключа
	Select(fdbx.Key) (fdbx.Pair, error)

	// Последовательная выборка всех активных ключей в диапазоне
	// Поддерживает опции From, To, Reverse, Limit, PackSize, Exclusive, Writer
	ListAll(...Option) ([]fdbx.Pair, error)

	// Последовательная выборка всех активных ключей в диапазоне
	// Поддерживает опции From, To, Reverse, Limit, PackSize, Exclusive, Writer
	SeqScan(ctx context.Context, args ...Option) (<-chan fdbx.Pair, <-chan error)

	// Загрузка бинарных данных по ключу, указывается ожидаемый размер
	LoadBLOB(fdbx.Key, int, ...Option) ([]byte, error)
}

// Begin - создание и старт новой транзакции
func Begin(conn db.Connection) (Tx, error) { return newTx64(conn, false) }

// Tx - объект "логической" транзакции MVCC поверх "физической" транзакции FDB
type Tx interface {
	// То же самое, что и в транзакции на чтение
	ReadTx

	// Успешное завершение (принятие) транзакции
	// Перед завершением выполняет хуки OnCommit
	// Поддерживает опции Writer
	Commit(args ...Option) error

	// Неудачное завершение (отклонение) транзакции
	// Поддерживает опции Writer
	Cancel(args ...Option) error

	// Удаление значения для ключа
	// Поддерживает опции Writer
	Delete([]fdbx.Key, ...Option) error

	// Вставка или обновление значения для ключа
	// Поддерживает опции Writer
	Upsert([]fdbx.Pair, ...Option) error

	// Удаление бинарных данных по ключу
	// Поддерживает опции Writer
	DropBLOB(fdbx.Key, ...Option) error

	// Сохранение бинарных данных по ключу
	SaveBLOB(fdbx.Key, []byte, ...Option) error

	// Регистрация хука для выполнения при завершении транзакции
	OnCommit(CommitHandler)

	// Запуск очистки устаревших записей ключей по указанному префиксу
	Vacuum(fdbx.Key, ...Option) (err error)
}

// Option - дополнительный аргумент при выполнении команды
type Option func(*options)

// Handler - обработчик события операции с записью
type Handler func(Tx, fdbx.Pair) error

// RowHandler - обработчик события операции с записью в рамках физической транзакции
type RowHandler func(Tx, fdbx.Pair, db.Writer) error

// CommitHandler - обработчик события завершения логической транзакции
type CommitHandler func(db.Writer) error

// Ошибки модуля
var (
	ErrWrite    = errx.New("Модифицикация в транзакции только для чтения")
	ErrBegin    = errx.New("Ошибка старта транзакции")
	ErrClose    = errx.New("Ошибка завершения транзакции")
	ErrSelect   = errx.New("Ошибка получения данных")
	ErrUpsert   = errx.New("Ошибка обновления данных")
	ErrDelete   = errx.New("Ошибка удаления данных")
	ErrSeqScan  = errx.New("Ошибка полной выборки данных")
	ErrNotFound = errx.New("Отсутствует значение")
	ErrBLOBLoad = errx.New("Ошибка загрузки BLOB")
	ErrBLOBDrop = errx.New("Ошибка удаления BLOB")
	ErrBLOBSave = errx.New("Ошибка сохранения BLOB")
	ErrWatch    = errx.New("Ошибка отслеживания значения")
	ErrVacuum   = errx.New("Ошибка автоочистки значений")
)
