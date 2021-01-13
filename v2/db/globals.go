package db

import (
	"bytes"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/errx"
)

var tail fdb.Key = bytes.Repeat([]byte{0xFF}, 256)

// Ошибки модуля
var (
	ErrWait    = errx.New("Ошибка ожидания значения")
	ErrRead    = errx.New("Ошибка транзакции чтения")
	ErrWrite   = errx.New("Ошибка транзакции записи")
	ErrClear   = errx.New("Ошибка транзакции очистки")
	ErrConnect = errx.New("Ошибка подключения к FoundationDB")
)
