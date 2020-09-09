package orm

import (
	"sync/atomic"

	"github.com/shestakovda/fdbx"
)

func Count(counter *uint64) AggFunc {
	return func(fdbx.Pair) error {
		atomic.AddUint64(counter, 1)
		return nil
	}
}
