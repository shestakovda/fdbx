package db

import "github.com/apple/foundationdb/bindings/go/src/fdb"

func NewBytePair(ns byte, key, val []byte) Pair {
	return &bytePair{
		ns:  ns,
		key: fdb.Key(key),
		val: val,
	}
}

type bytePair struct {
	ns  byte
	key fdb.Key
	val []byte
}

func (p *bytePair) NS() byte      { return p.ns }
func (p *bytePair) Key() []byte   { return p.key }
func (p *bytePair) Value() []byte { return p.val }
