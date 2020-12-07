package fdbx

import (
	"context"

	fbs "github.com/google/flatbuffers/go"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/errx"
)

// Key - некоторый ключ в БД, может использоваться в разных контекстах, допускает преобразования
type Key interface {
	Raw() fdb.Key
	Bytes() []byte
	String() string
	LSkip(uint16) Key
	RSkip(uint16) Key
	LPart(...byte) Key
	RPart(...byte) Key
	Clone() Key
}

// Bytes2Key - формирование простого ключа
func Bytes2Key(k []byte) Key { return fdbKey(k) }

// String2Key - формирование простого ключа из строки
func String2Key(k string) Key { return fdbKey(k) }

// Pair - пара ключ/значение, без возможности трансформации
type Pair interface {
	Key() Key
	Value() []byte
	Unwrap() Pair
}

// NewPair - новая простая пара, без всяких выкрутасов
func NewPair(k Key, v []byte) Pair {
	return &simplePair{
		k: k,
		v: v,
	}
}

// WrapPair - новая простая пара, без всяких выкрутасов
func WrapPair(k Key, p Pair) Pair {
	return &wrapPair{
		k: k,
		p: p,
	}
}

// ListGetter - метод для отложенного получения списка значений
type ListGetter interface {
	Resolve() []Pair
}

// Waiter - объект ожидания изменения ключа
type Waiter interface {
	Resolve(context.Context) error
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
