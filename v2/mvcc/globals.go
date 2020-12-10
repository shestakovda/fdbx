package mvcc

import (
	"encoding/binary"
	"time"

	"github.com/shestakovda/fdbx/v2"
	"github.com/sony/sonyflake"
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

func txKey(x uint64) fdbx.Key {
	var txid [9]byte
	txid[0] = nsTx
	binary.BigEndian.PutUint64(txid[1:], x)
	return fdbx.Bytes2Key(txid[:])
}

var sflake = sonyflake.NewSonyflake(sonyflake.Settings{
	StartTime: time.Now(),
})
