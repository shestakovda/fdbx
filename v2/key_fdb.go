package fdbx

import "github.com/apple/foundationdb/bindings/go/src/fdb"

type fdbKey fdb.Key

func (k fdbKey) Raw() fdb.Key      { return fdb.Key(k) }
func (k fdbKey) Bytes() []byte     { return []byte(k) }
func (k fdbKey) String() string    { return string(k) }
func (k fdbKey) Printable() string { return fdb.Key(k).String() }

func (k fdbKey) LSkip(i uint16) Key {
	if i >= uint16(len(k)) {
		return Bytes2Key(nil)
	}

	return Bytes2Key(k[i:])
}

func (k fdbKey) RSkip(i uint16) Key {
	if i >= uint16(len(k)) {
		return Bytes2Key(nil)
	}

	return Bytes2Key(k[:len(k)-int(i)])
}

func (k fdbKey) LPart(part ...byte) Key {
	return Bytes2Key(part).RPart(k...)
}

func (k fdbKey) RPart(part ...byte) Key {
	k2 := make(fdbKey, len(k)+len(part))
	copy(k2[:len(k)], k)
	copy(k2[len(k):], part)
	return k2
}

func (k fdbKey) Clone() Key {
	k2 := make(fdbKey, len(k))
	copy(k2, k)
	return k2
}
