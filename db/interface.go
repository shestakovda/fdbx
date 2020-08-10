package db

import (
	"context"

	"github.com/shestakovda/errors"
)

type Connection interface {
	Clear() error

	Read(func(Reader) error) error
	Write(func(Writer) error) error

	Serial(ctx context.Context, ns byte, from, to []byte, limit int, reverse bool) (<-chan *Pair, <-chan error)
}

type Reader interface {
	Pair(ns byte, key []byte) (*Pair, error)
	List(ns byte, from, to []byte, limit int, reverse bool) (ListPromise, error)
}

type Writer interface {
	Reader

	SetPair(ns byte, key, value []byte) error
	SetVersion(ns byte, key []byte) error

	DropPair(ns byte, key []byte) error
}

type Pair struct {
	Key   []byte
	Value []byte
}

type ListPromise interface {
	Resolve() []*Pair
}

var (
	ErrRead    = errors.New("read")
	ErrWrite   = errors.New("write")
	ErrClear   = errors.New("clear")
	ErrSerial  = errors.New("serial")
	ErrConnect = errors.New("connection")

	ErrBadDB = errors.New("bad DB")
	ErrBadNS = errors.New("bad NS")

	ErrGetPair = errors.New("get pair")
	ErrGetList = errors.New("get list")

	ErrSetPair    = errors.New("set pair")
	ErrSetVersion = errors.New("set version")

	ErrDropPair = errors.New("drop pair")
)
