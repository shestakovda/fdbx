package fdbx

import "github.com/apple/foundationdb/bindings/go/src/fdb"

type Key fdb.Key
type Value []byte

type KeyWrapper func(Key) Key
type ValueWrapper func(Value) Value

type Pair interface {
	Key() Key
	Value() Value

	WrapKey(KeyWrapper) Pair
	WrapValue(ValueWrapper) Pair
}
