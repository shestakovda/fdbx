package orm

import "sync/atomic"

func Count(counter *uint64) AggFunc {
	return func(Row) error {
		atomic.AddUint64(counter, 1)
		return nil
	}
}
