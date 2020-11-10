package fdbx

import (
	"context"

	fbs "github.com/google/flatbuffers/go"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/errx"
)

// Key - некоторый ключ в БД, может использоваться в разных контекстах
type Key fdb.Key

// KeyWrapper - преобразователь ключа, может делать с ним любые трансформации
type KeyWrapper func(Key) (Key, error)

// ValueWrapper - преобразователь значения, может делать с ним любые трансформации
type ValueWrapper func([]byte) ([]byte, error)

// Pair - пара ключ/значение, с возможностью трансформации
type Pair interface {
	Raw() []byte

	Key() (Key, error)
	Value() ([]byte, error)

	Clone() Pair
	Apply() error

	WrapKey(KeyWrapper) Pair
	WrapValue(ValueWrapper) Pair
}

// ListGetter - метод для отложенного получения списка значений
type ListGetter interface {
	Resolve() []Pair
}

// Waiter - объект ожидания изменения ключа
type Waiter interface {
	Resolve(context.Context) error
}

// KeyManager - интерфейс управления ключами
type KeyManager interface {
	Wrap(Key) Key
	Unwrap(Key) Key

	Wrapper(Key) (Key, error)
	Unwrapper(Key) (Key, error)
}

// FlatPacker - интерфейс для упаковки flatbuffers
type FlatPacker interface {
	Pack(*fbs.Builder) fbs.UOffsetT
}

var (
	ErrKey       = errx.New("Ошибка загрузки ключа")
	ErrValue     = errx.New("Ошибка загрузки значения")
	ErrByte2Time = errx.New("Ошибка преобразования значения во время")
)
