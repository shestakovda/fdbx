package mvcc

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/shestakovda/fdbx/v2/db"
)

func getOpts(args []Option) (o options) {
	// Объем выборки "грязных" записей в процессе вакуума
	o.vpack = 10000

	// Объем выборки "грязных" записей в процессе работы
	o.spack = 0

	// Максимальное кол-во байт, которое могут занимать строки, сохраняемые в рамках одной физической транзакции
	o.rowmem = 9000000

	// Максимальное кол-во байт, которое может занимать "чистое" значение ключа, с запасом на накладные расходы
	o.rowsize = 90000

	for i := range args {
		args[i](&o)
	}

	return
}

type options struct {
	lock     bool
	reverse  bool
	physical bool
	limit    int
	rowmem   int
	rowsize  int
	vpack    uint64
	spack    uint64
	from     fdb.Key
	last     fdb.Key
	onInsert RowHandler
	onUpdate RowHandler
	onDelete RowHandler
	onVacuum RowHandler
	onLock   RowHandler
	writer   db.Writer
}

func Lock() Option                    { return func(o *options) { o.lock = true } }
func Last(k fdb.Key) Option           { return func(o *options) { o.last = k } }
func From(k fdb.Key) Option           { return func(o *options) { o.from = k } }
func Limit(l int) Option              { return func(o *options) { o.limit = l } }
func Writer(w db.Writer) Option       { return func(o *options) { o.writer = w } }
func Reverse() Option                 { return func(o *options) { o.reverse = true } }
func Physical() Option                { return func(o *options) { o.physical = true } }
func OnInsert(hdl RowHandler) Option  { return func(o *options) { o.onInsert = hdl } }
func OnUpdate(hdl RowHandler) Option  { return func(o *options) { o.onUpdate = hdl } }
func OnDelete(hdl RowHandler) Option  { return func(o *options) { o.onDelete = hdl } }
func OnVacuum(hdl RowHandler) Option  { return func(o *options) { o.onVacuum = hdl } }
func Exclusive(hdl RowHandler) Option { return func(o *options) { o.lock = true; o.onLock = hdl } }
func SelectPack(size int) Option      { return func(o *options) { o.spack = uint64(size) } }
func MaxRowMem(size int) Option       { return func(o *options) { o.rowmem = size } }
func MaxRowSize(size int) Option      { return func(o *options) { o.rowsize = size } }
