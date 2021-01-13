package mvcc

import (
	"encoding/binary"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/models"
)

func sysPair(opid uint32, key fdb.Key, txid, data []byte) fdb.KeyValue {
	var part [16]byte
	copy(part[:8], txid[:8])
	copy(part[12:16], txid[8:12])
	binary.BigEndian.PutUint32(part[8:12], opid)

	return fdb.KeyValue{
		fdbx.AppendRight(WrapKey(key), part[:]...),
		fdbx.FlatPack(&models.RowT{Data: data}),
	}
}
