package db

import "github.com/shestakovda/errors"

type Namespace byte

type Connection interface {
	Read(func(Reader) error) error
	Write(func(Writer) error) error
}

type Reader interface {
	Pair(ns Namespace, key []byte) (*Pair, error)
	List(ns Namespace, prefix []byte, limit int, reverse bool) ([]*Pair, error)
}

type Writer interface {
	Reader

	SetPair(ns Namespace, key, value []byte) error
	SetVersion(ns Namespace, key []byte) error
}

type Pair struct {
	NS    Namespace
	Key   []byte
	Value []byte
}

var (
	ErrRead    = errors.New("read")
	ErrWrite   = errors.New("write")
	ErrConnect = errors.New("connection")

	ErrBadDB = errors.New("bad DB")
	ErrBadNS = errors.New("bad NS")

	ErrGetPair = errors.New("get pair")
	ErrGetList = errors.New("get list")

	ErrSetPair    = errors.New("set pair")
	ErrSetVersion = errors.New("set version")
)
