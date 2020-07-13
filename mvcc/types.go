package mvcc

import (
	"reflect"
	"unsafe"
)

type strBytes string

func (s strBytes) String() string { return string(s) }

func (s strBytes) Bytes() []byte {
	if s == "" {
		return nil
	}

	return *(*[]byte)(unsafe.Pointer(&reflect.SliceHeader{
		Data: (*reflect.StringHeader)(unsafe.Pointer(&s)).Data,
		Len:  len(s),
		Cap:  len(s),
	}))
}
