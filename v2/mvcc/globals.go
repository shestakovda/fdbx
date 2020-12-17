package mvcc

import (
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid"
	"github.com/shestakovda/fdbx/v2"
)

// Переменные модуля, менять которые не рекомендуется
var (
	// Максимальное кол-во байт, которое могут занимать строки, сохраняемые в рамках одной физической транзакции
	MaxRowMem = 9000000
	// Максимальное кол-во байт, которое может занимать "чистое" значение ключа, с запасом на накладные расходы
	MaxRowSize = 90000
	// Максимальное число "грязных" строк, выбираемых в одной физической транзакции
	MaxRowCount = 100
)

var globCache = makeCache()

const (
	nsUser  byte = 0
	nsTx    byte = 1
	nsLock  byte = 2
	nsWatch byte = 3
)

const (
	txStatusUnknown   byte = 0
	txStatusRunning   byte = 1
	txStatusAborted   byte = 2
	txStatusCommitted byte = 3
)

func txKey(txid []byte) fdbx.Key {
	return fdbx.Bytes2Key(txid).RPart(nsTx)
}

var entropy = sync.Pool{
	New: func() interface{} {
		return ulid.Monotonic(rand.New(rand.NewSource(time.Now().UnixNano())), 0)
	},
}
