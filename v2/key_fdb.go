package fdbx

import "github.com/apple/foundationdb/bindings/go/src/fdb"

func SkipLeft(k fdb.Key, i int) fdb.Key {
	if i >= len(k) {
		return nil
	}

	return k[i:]
}

func SkipRight(k fdb.Key, i int) fdb.Key {
	if i >= len(k) {
		return nil
	}

	return k[:len(k)-i]
}

func AppendLeft(k fdb.Key, part ...byte) fdb.Key {
	return AppendRight(part, k...)
}

func AppendRight(k fdb.Key, part ...byte) fdb.Key {
	k2 := make(fdb.Key, len(k)+len(part))
	copy(k2[:len(k)], k)
	copy(k2[len(k):], part)
	return k2
}
