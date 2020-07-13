package mvcc

import "github.com/shestakovda/fdbx/db"

var (
	txCache = newStatusCache()
)

type txStatus byte

const (
	txStatusUnknown   txStatus = 0
	txStatusRunning   txStatus = 1
	txStatusAborted   txStatus = 2
	txStatusCommitted txStatus = 3
)

const (
	nsUser  db.Namespace = 0
	nsTx    db.Namespace = 1
	nsTxTmp db.Namespace = 2
	nsBLOB  db.Namespace = 3
)
