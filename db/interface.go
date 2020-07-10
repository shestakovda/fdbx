package db

import "github.com/shestakovda/errors"

type Connection interface {
	Read(func(Reader) error) error
	Write(func(Writer) error) error
}

type Reader interface {
	Pair(namespace byte, key []byte) (Pair, error)
	List(namespace byte, prefix []byte, reverse bool) ([]Pair, error)
}

type Writer interface {
	Reader

	SetPair(p Pair) error
	SetVersion(p Pair) error
}

type Pair interface {
	NS() byte
	Key() []byte
	Value() []byte
}

var (
	ErrRead    = errors.New("read error")
	ErrWrite   = errors.New("write error")
	ErrSchema  = errors.New("schema error")
	ErrConnect = errors.New("connection error")
)
