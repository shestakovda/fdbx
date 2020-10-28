package mvcc

import (
	"sync"

	"github.com/shestakovda/fdbx/v2/models"

	fbs "github.com/google/flatbuffers/go"
)

// Переменные модуля, менять которые не рекомендуется
var (
	// Максимальное кол-во байт, которое могут занимать строки, сохраняемые в рамках одной физической транзакции
	MaxRowMem = 9000000
	// Максимальное кол-во байт, которое может занимать "чистое" значение ключа, с запасом на накладные расходы
	MaxRowSize = 90000
	// Максимальное число "грязных" строк, выбираемых в одной физической транзакции
	MaxRowCount = uint64(100000)
)

var globCache = makeCache()
var keyMgr = NewTxKeyManager()

var fbsPool = sync.Pool{New: func() interface{} { return fbs.NewBuilder(128) }}
var hdlPool = sync.Pool{New: func() interface{} { return make([]CommitHandler, 0, 16) }}

const (
	nsUser  byte = 0
	nsTx    byte = 1
	nsTxTmp byte = 2
)

const (
	txStatusUnknown   byte = 0
	txStatusRunning   byte = 1
	txStatusAborted   byte = 2
	txStatusCommitted byte = 3
)

func valWrapper(v []byte) ([]byte, error) {
	if len(v) == 0 {
		return nil, nil
	}
	return models.GetRootAsRow(v, 0).DataBytes(), nil
}
