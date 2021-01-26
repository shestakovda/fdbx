package orm

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

const (
	nsData  byte = 0
	nsBLOB  byte = 1
	nsIndex byte = 2
	nsQueue byte = 3
	nsQuery byte = 5
)

const (
	qFlag byte = 0
	qList byte = 1
	qWork byte = 2
	qMeta byte = 3
)

// Ограничение в 100Кб, но берем небольшой запас на накладные расходы
var loLimit = 90000

var qTriggerKey = fdb.Key("trigger")
