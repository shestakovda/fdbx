package orm

import (
	"sync/atomic"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func Count(counter *uint64) Aggregator {
	return func(value fdb.KeyValue) error {
		atomic.AddUint64(counter, 1)
		return nil
	}
}
