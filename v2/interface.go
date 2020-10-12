package fdbx

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/errx"
)

type Key fdb.Key
type Value []byte

type KeyWrapper func(Key) (Key, error)
type ValueWrapper func(Value) (Value, error)

type Pair interface {
	Key() (Key, error)
	Value() (Value, error)

	WrapKey(KeyWrapper) Pair
	WrapValue(ValueWrapper) Pair
}

var (
	ErrKey   = errx.New("Ошибка загрузки ключа")
	ErrValue = errx.New("Ошибка загрузки значения")
)
