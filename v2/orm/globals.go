package orm

import "github.com/shestakovda/fdbx/v2"

const (
	nsData  byte = 0
	nsBLOB  byte = 1
	nsIndex byte = 2
	nsQueue byte = 3
	nsWatch byte = 4
	nsQuery byte = 5
)

const (
	qFlag byte = 0
	qList byte = 1
	qWork byte = 2
	qMeta byte = 3
)

var loLimit int = 100000

var qTriggerKey = fdbx.String2Key("trigger")
var qTotalWaitKey = fdbx.String2Key("wait")
var qTotalWorkKey = fdbx.String2Key("work")
