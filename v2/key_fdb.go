package fdbx

import "github.com/apple/foundationdb/bindings/go/src/fdb"

type fdbKey fdb.Key

func (k fdbKey) Raw() fdb.Key      { return fdb.Key(k) }
func (k fdbKey) Bytes() []byte     { return []byte(k) }
func (k fdbKey) String() string    { return string(k) }
func (k fdbKey) Printable() string { return fdb.Key(k).String() }

func (k fdbKey) LSkip(i uint16) Key {
	if i >= uint16(len(k)) {
		k = nil
	} else {
		k = k[i:]
	}
	return k
}

func (k fdbKey) RSkip(i uint16) Key {
	if i >= uint16(len(k)) {
		k = nil
	} else {
		k = k[:len(k)-int(i)]
	}
	return k
}

func (k fdbKey) LPart(part ...byte) Key {
	return fdbKey(append(part, k...))
}

func (k fdbKey) RPart(part ...byte) Key {
	return append(k, part...)
}

func (k fdbKey) Clone() Key {
	k2 := make(fdbKey, len(k))
	copy(k2, k)
	return k2
}
