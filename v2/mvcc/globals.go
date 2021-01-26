package mvcc

import (
	"encoding/binary"
	"math/rand"
	"time"
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
	txStatusCancelled byte = 1
	txStatusRunning   byte = 2
	txStatusCommitted byte = 3
)

type suid [12]byte

func newTxID() (uid suid) {
	now := time.Now().UTC().UnixNano()
	binary.BigEndian.PutUint64(uid[:8], uint64(now))
	binary.BigEndian.PutUint32(uid[8:12], rand.New(rand.NewSource(now)).Uint32())
	return
}
