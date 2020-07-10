package mvcc

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

type txNamespace byte

const (
	nsUser  txNamespace = 0
	nsTx    txNamespace = 1
	nsTxTmp txNamespace = 2
	nsBLOB  txNamespace = 3
)
