package mvcc

import (
	"context"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
)

// Debug - флаг отладочных принтов
var Debug = false

// TxCacheSize - размер глобального кеша статусов завершенных транзакций
var TxCacheSize = 8000000

// Begin - создание и старт новой транзакции
func Begin(dbc db.Connection) Tx { return newTx64(dbc) }

// WithTx - выполнение метода в рамках транзакции
func WithTx(dbc db.Connection, hdl TxHandler) (err error) {
	tx := Begin(dbc)
	defer tx.Cancel()

	if err = hdl(tx); err != nil {
		return
	}

	return tx.Commit()
}

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
	Select(fdb.Key, ...Option) (fdb.KeyValue, error)

	// Удаление значения для ключа
	// Поддерживает опции Writer
	Delete([]fdb.Key, ...Option) error

	// Вставка или обновление значения для ключа
	// Поддерживает опции Writer
	Upsert([]fdb.KeyValue, ...Option) error

	// Последовательная выборка всех активных ключей в диапазоне
	// Поддерживает опции From, To, Reverse, Limit, PackSize, Exclusive, Writer
	ListAll(context.Context, ...Option) ([]fdb.KeyValue, error)

	// Последовательная выборка всех активных ключей в диапазоне
	// Поддерживает опции From, To, Reverse, Limit, PackSize, Exclusive, Writer
	SeqScan(context.Context, ...Option) (<-chan fdb.KeyValue, <-chan error)

	// Загрузка бинарных данных по ключу, указывается ожидаемый размер
	LoadBLOB(fdb.Key, int, ...Option) ([]byte, error)

	// Удаление бинарных данных по ключу
	// Поддерживает опции Writer
	DropBLOB(fdb.Key, ...Option) error

	// Сохранение бинарных данных по ключу
	SaveBLOB(fdb.Key, []byte, ...Option) error

	// Блокировка записи с доступом на чтение по сигнальному ключу
	SharedLock(fdb.Key, time.Duration) error

	// Регистрация хука для выполнения при удачном завершении транзакции
	OnCommit(CommitHandler)

	// Запуск очистки устаревших записей ключей по указанному префиксу
	Vacuum(fdb.Key, ...Option) error

	// Изменение сигнального ключа, чтобы сработали Watch
	// По сути, выставляет хук OnCommit с правильным содержимым
	Touch(fdb.Key)

	// Ожидание изменения сигнального ключа в Touch
	Watch(fdb.Key) (fdbx.Waiter, error)
}

// Option - дополнительный аргумент при выполнении команды
type Option func(*options)

// TxHandler - обработчик события операции с записью
type TxHandler func(Tx) error

// RowHandler - обработчик события операции с записью в рамках физической транзакции
type RowHandler func(Tx, db.Writer, fdb.KeyValue) error

// CommitHandler - обработчик события завершения логической транзакции
type CommitHandler func(db.Writer) error

// WrapKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapKey(key fdb.Key) fdb.Key {
	return fdbx.AppendLeft(key, nsUser)
}

// UnwrapKey - обертка ключа для получения пользовательского ключа из системного, при загрузке
func UnwrapKey(key fdb.Key) fdb.Key {
	return fdbx.SkipRight(fdbx.SkipLeft(key, 1), 16)
}

// WrapTxKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapTxKey(key fdb.Key) fdb.Key {
	return fdbx.AppendLeft(key, nsTx)
}

// WrapLockKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapLockKey(key fdb.Key) fdb.Key {
	return fdbx.AppendLeft(key, nsLock)
}

// WrapWatchKey - обертка для получения системного ключа из пользовательского, при сохранении
func WrapWatchKey(key fdb.Key) fdb.Key {
	return fdbx.AppendLeft(key, nsWatch)
}

// Ошибки модуля
var (
	ErrWrite      = errx.New("Модифицикация в транзакции только для чтения")
	ErrBegin      = errx.New("Ошибка старта транзакции")
	ErrClose      = errx.New("Ошибка завершения транзакции")
	ErrSelect     = errx.New("Ошибка получения данных")
	ErrUpsert     = errx.New("Ошибка обновления данных")
	ErrDelete     = errx.New("Ошибка удаления данных")
	ErrSeqScan    = errx.New("Ошибка полной выборки данных")
	ErrNotFound   = errx.New("Отсутствует значение")
	ErrDuplicate  = errx.New("Дублирующее значение")
	ErrBLOBLoad   = errx.New("Ошибка загрузки BLOB")
	ErrBLOBDrop   = errx.New("Ошибка удаления BLOB")
	ErrBLOBSave   = errx.New("Ошибка сохранения BLOB")
	ErrSharedLock = errx.New("Ошибка получения блокировки")
	ErrDeadlock   = errx.New("Ошибка ожидания освобождения блокировки")
	ErrWatch      = errx.New("Ошибка отслеживания значения")
	ErrVacuum     = errx.New("Ошибка автоочистки значений")
)
