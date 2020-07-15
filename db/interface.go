package db

import "github.com/shestakovda/errors"

type Connection interface {
	Read(func(Reader) error) error
	Write(func(Writer) error) error
	Clear() error
}

type Reader interface {
	Pair(ns byte, key []byte) (*Pair, error)
	List(ns byte, prefix []byte, limit int, reverse bool) ([]*Pair, error)
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

var (
	ErrRead    = errors.New("read")
	ErrWrite   = errors.New("write")
	ErrClear   = errors.New("clear")
	ErrConnect = errors.New("connection")

	ErrBadDB = errors.New("bad DB")
	ErrBadNS = errors.New("bad NS")

	ErrGetPair = errors.New("get pair")
	ErrGetList = errors.New("get list")

	ErrSetPair    = errors.New("set pair")
	ErrSetVersion = errors.New("set version")

	ErrDropPair = errors.New("drop pair")
)
