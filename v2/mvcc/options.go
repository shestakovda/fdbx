package mvcc

func getOpts(args []Option) (o options) {
	for i := range args {
		args[i](&o)
	}
	return
}

type options struct {
	onDelete Handler
	onInsert Handler
}

func OnDelete(hdl Handler) Option { return func(o *options) { o.onDelete = hdl } }
func OnInsert(hdl Handler) Option { return func(o *options) { o.onInsert = hdl } }
