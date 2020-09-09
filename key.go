package fdbx

import "github.com/apple/foundationdb/bindings/go/src/fdb"

func (k Key) Getter() Key { return k }

func (k Key) Bytes() fdb.Key { return fdb.Key(k) }

func (k Key) String() string { return fdb.Key(k).String() }

func (k Key) LSkip(i uint16) Key {
	if i >= uint16(len(k)) {
		k = nil
	} else {
		k = k[i:]
	}
	return k
}

func (k Key) RSkip(i uint16) Key {
	if i >= uint16(len(k)) {
		k = nil
	} else {
		k = k[:len(k)-int(i)]
	}
	return k
}

func (k Key) LPart(part ...byte) Key {
	k = append(part, k...)
	return k
}

func (k Key) RPart(part ...byte) Key {
	k = append(k, part...)
	return k
}
