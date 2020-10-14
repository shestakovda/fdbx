package mvcc

func getOpts(args []Option) (o options) {
	for i := range args {
		args[i](&o)
	}
	return
}

type options struct {
	lock     bool
	limit    int
	onDelete Handler
	onInsert Handler
}

func Exclusive() Option           { return func(o *options) { o.lock = true } }
func Limit(l int) Option          { return func(o *options) { o.limit = l } }
func OnDelete(hdl Handler) Option { return func(o *options) { o.onDelete = hdl } }
func OnInsert(hdl Handler) Option { return func(o *options) { o.onInsert = hdl } }
