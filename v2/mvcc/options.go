package mvcc

import (
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
)

func getOpts(args []Option) (o options) {
	for i := range args {
		args[i](&o)
	}

	return
}

type options struct {
	lock     bool
	reverse  bool
	limit    int
	packSize int
	from     fdbx.Key
	to       fdbx.Key
	onDelete Handler
	onInsert Handler
	onVacuum RowHandler
	onLock   RowHandler
	writer   db.Writer
}

func To(k fdbx.Key) Option            { return func(o *options) { o.to = k } }
func From(k fdbx.Key) Option          { return func(o *options) { o.from = k } }
func Limit(l int) Option              { return func(o *options) { o.limit = l } }
func Writer(w db.Writer) Option       { return func(o *options) { o.writer = w } }
func OnDelete(hdl Handler) Option     { return func(o *options) { o.onDelete = hdl } }
func OnInsert(hdl Handler) Option     { return func(o *options) { o.onInsert = hdl } }
func OnVacuum(hdl RowHandler) Option  { return func(o *options) { o.onVacuum = hdl } }
func Exclusive(hdl RowHandler) Option { return func(o *options) { o.lock = true; o.onLock = hdl } }
func Reverse() Option                 { return func(o *options) { o.reverse = true } }
