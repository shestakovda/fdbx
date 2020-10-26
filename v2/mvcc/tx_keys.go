package mvcc

import (
	"encoding/binary"

	"github.com/shestakovda/fdbx/v2"
)

func NewTxKeyManager() fdbx.KeyManager {
	return new(txKeyManager)
}

type txKeyManager struct{}

func (m txKeyManager) Wrap(k fdbx.Key) fdbx.Key {
	return k.LPart(nsUser)
}

func (m txKeyManager) Unwrap(k fdbx.Key) fdbx.Key {
	return k.LSkip(1).RSkip(8)
}

func (m txKeyManager) Wrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Wrap(k), nil
}

func (m txKeyManager) Unwrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Unwrap(k), nil
}

func txKey(x uint64) fdbx.Key {
	var txid [9]byte
	txid[0] = nsTx
	binary.BigEndian.PutUint64(txid[1:], x)
	return txid[:]
}
