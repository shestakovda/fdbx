package rpc

import (
	"context"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/orm"
)

// TaskHandler - обработчик задачи из очереди
type TaskHandler func(fdbx.Pair) error

// ErrorHandler - обработчик ошибки с возможностью перезапуска задачи
type ErrorHandler func(error) (bool, time.Duration)

// Listener - параметры
type Listener struct {
	Queue    orm.Queue
	OnTask   TaskHandler
	OnError  ErrorHandler
	OnListen ErrorHandler
}

// NewServer - конструктор сервера
func NewServer(list ...*Listener) Server { return newServerV1(list) }

// Server - глобальная служба, которая слушает задачи по всем очередям и запускает их обработку
type Server interface {
	Run(context.Context, db.Connection) error
	Stop()
}

// Ошибки модуля
var (
	ErrListen      = errx.New("Ошибка обработки очереди")
	ErrBadListener = errx.New("Ошибка регистрации обработчика")
)
