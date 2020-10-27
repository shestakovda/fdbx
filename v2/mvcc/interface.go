package mvcc

import (
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
)

// TxCacheSize - размер глобального кеша статусов завершенных транзакций
var TxCacheSize = 8000000

// Begin - создание и старт новой транзакции
func Begin(conn db.Connection) (Tx, error) { return newTx64(conn) }

// Tx - объект "логической" транзакции MVCC поверх "физической" транзакции FDB
type Tx interface {
	// Ссылка на подключение к БД, на всякий случай
	Conn() db.Connection

	// Успешное завершение (принятие) транзакции
	// Перед завершением выполняет хуки OnCommit
	// Поддерживает опции Writer
	Commit(args ...Option) error

	// Неудачное завершение (отклонение) транзакции
	// Поддерживает опции Writer
	Cancel(args ...Option) error

	// Выборка актуального значения для ключа
	Select(fdbx.Key) (fdbx.Pair, error)

	// Удаление значения для ключа
	// Поддерживает опции Writer
	Delete([]fdbx.Key, ...Option) error

	// Вставка или обновление значения для ключа
	// Поддерживает опции Writer
	Upsert([]fdbx.Pair, ...Option) error

	// Последовательная выборка всех активных ключей в диапазоне
	// Поддерживает опции Writer, Limit, PackSize, Exclusive
	SeqScan(from, to fdbx.Key, args ...Option) ([]fdbx.Pair, error)

	// Удаление бинарных данных по ключу
	// Поддерживает опции Writer
	DropBLOB(fdbx.Key, ...Option) error

	// Сохранение бинарных данных по ключу
	SaveBLOB(fdbx.Key, []byte, ...Option) error

	// Загрузка бинарных данных по ключу, указывается ожидаемый размер
	LoadBLOB(fdbx.Key, int, ...Option) ([]byte, error)

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
