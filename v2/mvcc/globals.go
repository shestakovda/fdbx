package mvcc

import (
	"encoding/binary"
	"sync"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"

	fbs "github.com/google/flatbuffers/go"
)

var globCache = makeCache()

var fbsPool = sync.Pool{New: func() interface{} { return fbs.NewBuilder(128) }}

const (
	nsUser  byte = 0
	nsTx    byte = 1
	nsTxTmp byte = 2
)

const (
	txStatusUnknown   byte = 0
	txStatusRunning   byte = 1
	txStatusAborted   byte = 2
	txStatusCommitted byte = 3
)

func txKey(x uint64) fdbx.Key {
	var txid [9]byte
	txid[0] = nsTx
	binary.BigEndian.PutUint64(txid[1:], x)
	return txid[:]
}

func sysWrapper(key fdbx.Key) (fdbx.Key, error) { return key.LSkip(1), nil }

func usrWrapper(key fdbx.Key) (fdbx.Key, error) { return key.LPart(nsUser), nil }

func valWrapper(v fdbx.Value) (fdbx.Value, error) {
	if len(v) == 0 {
		return nil, nil
	}
	return models.GetRootAsRow(v, 0).DataBytes(), nil
}
