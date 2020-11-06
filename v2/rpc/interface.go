package rpc

import (
	"context"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/orm"
)

// Суффиксы элементов
const (
	NSRequest  byte = 0
	NSResponse byte = 1
)

// TaskHandler - обработчик задачи из очереди
type TaskHandler func(orm.Task) ([]byte, error)

// ErrorHandler - обработчик ошибки с возможностью перезапуска задачи
type ErrorHandler func(orm.Task, error) (bool, time.Duration, []byte, error)

// ListenHandler - обработчик ошибки с возможностью перезапуска задачи
type ListenHandler func(error) (bool, time.Duration)

// Option - дополнительный параметр обработчика
type Option func(*options)

// NewServer - конструктор сервера
func NewServer(id uint16) Server { return newServerV1(id) }

// Server - глобальная служба, которая слушает задачи по всем очередям и запускает их обработку
type Server interface {
	Endpoint(id uint16, hdl TaskHandler, args ...Option) error
	Run(context.Context, db.Connection, ...Option)
	Stop()
}

// NewClient - конструктор клиента
func NewClient(cn db.Connection, srvID uint16) Client { return newClientV1(cn, srvID) }

// Client - локальный объект для выполнения удаленных операций
type Client interface {
	Result(fdbx.Key) ([]byte, error)
	SyncExec(ctx context.Context, endID uint16, req []byte, args ...Option) ([]byte, error)
}

// Ошибки модуля
var (
	ErrListen      = errx.New("Ошибка обработки очереди")
	ErrVacuum      = errx.New("Ошибка автоочистки синхронизатора")
	ErrBadListener = errx.New("Ошибка регистрации обработчика")
	ErrSyncExec    = errx.New("Ошибка синхронной обработки")
	ErrResult      = errx.New("Ошибка загрузки результата обработки")
	ErrConfirm     = errx.New("Ошибка подтверждения обработки")
	ErrRepeat      = errx.New("Ошибка регистрации повтора обработки")
)
