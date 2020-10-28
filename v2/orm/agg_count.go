package orm

import (
	"sync/atomic"

	"github.com/shestakovda/fdbx/v2"
)

func Count(counter *uint64) Aggregator {
	return func(fdbx.Pair) error {
		atomic.AddUint64(counter, 1)
		return nil
	}
}
