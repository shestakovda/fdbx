package mvcc

import (
	"sync"

	"github.com/oklog/ulid"
)

func makeCache() *txCache { return new(txCache) }

type txCache struct {
	sync.RWMutex
	cache map[ulid.ULID]byte
}

func (c *txCache) get(txid ulid.ULID) byte {
	c.RLock()
	defer c.RUnlock()
	return c.cache[txid]
}

func (c *txCache) set(txid ulid.ULID, status byte) {
	c.Lock()
	defer c.Unlock()

	if c.cache == nil || len(c.cache) > TxCacheSize {
		c.cache = make(map[ulid.ULID]byte, 256)
	}

	c.cache[txid] = status
}
