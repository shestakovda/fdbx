package mvcc

var (
	txCache = newStatusCache()
)

const (
	nsUser  byte = 0
	nsTx    byte = 1
	nsTxTmp byte = 2
	nsBLOB  byte = 3
)

const (
	txStatusUnknown   byte = 0
	txStatusRunning   byte = 1
	txStatusAborted   byte = 2
	txStatusCommitted byte = 3
)
