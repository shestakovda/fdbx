package mvcc

import (
	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx"
	"github.com/shestakovda/fdbx/db"
)

// TxCacheSize - размер глобального кеша статусов завершенных транзакций
var TxCacheSize = 8000000

// ScanRangeSize - кол-во строк в одной "физической" транзакции выборки
var ScanRangeSize uint64 = 10000

// Tx - объект "логической" транзакции MVCC поверх "физической" транзакции FDB
type Tx interface {
	Commit() error
	Cancel() error

	Select(fdbx.Key) (fdbx.Pair, error)
	Delete([]fdbx.Key, ...Option) error
	Upsert([]fdbx.Pair, ...Option) error

	SeqScan(from, to fdbx.Key) ([]fdbx.Pair, error)
}

// Option - дополнительный аргумент при выполнении команды
type Option func(*options)

// DeleteHandler - обработчик события удаления записи
type Handler func(Tx, fdbx.Pair) error

// Begin - создание и старт новой транзакции
func Begin(conn db.Connection) (Tx, error) { return newTx64(conn) }

// Ошибки модуля
var (
	ErrBegin   = errx.New("Ошибка старта транзакции")
	ErrClose   = errx.New("Ошибка завершения транзакции")
	ErrSelect  = errx.New("Ошибка получения данных")
	ErrUpsert  = errx.New("Ошибка обновления данных")
	ErrDelete  = errx.New("Ошибка удаления данных")
	ErrSeqScan = errx.New("Ошибка полной выборки данных")
)
