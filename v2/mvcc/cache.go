package mvcc

import (
	"sync"
)

func makeCache() *txCache { return new(txCache) }

type txCache struct {
	sync.RWMutex
	cache map[suid]byte
}

func (c *txCache) get(txid suid) byte {
	c.RLock()
	defer c.RUnlock()
	return c.cache[txid]
}

func (c *txCache) set(txid suid, status byte) {
	c.Lock()
	defer c.Unlock()

	if c.cache == nil || len(c.cache) > TxCacheSize {
		c.cache = make(map[suid]byte, 256)
	}

	c.cache[txid] = status
}
