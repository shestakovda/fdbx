package mvcc

import "github.com/shestakovda/fdbx/v2/db"

func getOpts(args []Option) (o options) {
	for i := range args {
		args[i](&o)
	}
	o.packSize = 10000
	return
}

type options struct {
	lock     bool
	reverse  bool
	limit    int
	packSize int
	onDelete Handler
	onInsert Handler
	onVacuum RowHandler
	onLock   RowHandler
	writer   db.Writer
}

func Limit(l int) Option              { return func(o *options) { o.limit = l } }
func Reverse() Option                 { return func(o *options) { o.reverse = true } }
func Writer(w db.Writer) Option       { return func(o *options) { o.writer = w } }
func PackSize(s int) Option           { return func(o *options) { o.packSize = s } }
func OnDelete(hdl Handler) Option     { return func(o *options) { o.onDelete = hdl } }
func OnInsert(hdl Handler) Option     { return func(o *options) { o.onInsert = hdl } }
func OnVacuum(hdl RowHandler) Option  { return func(o *options) { o.onVacuum = hdl } }
func Exclusive(hdl RowHandler) Option { return func(o *options) { o.lock = true; o.onLock = hdl } }
