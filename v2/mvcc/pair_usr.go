package mvcc

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/shestakovda/fdbx/v2/models"
)

func usrPair(orig fdb.KeyValue) fdb.KeyValue {
	orig.Key = UnwrapKey(orig.Key)
	if len(orig.Value) > 0 {
		orig.Value = models.GetRootAsRow(orig.Value, 0).DataBytes()
	}
	return orig
}
