package mvcc

import (
	"sync"
)

func makeCache() *txCache { return new(txCache) }

type txCache struct {
	sync.RWMutex
	cache map[string]byte
}

func (c *txCache) get(txid string) byte {
	c.RLock()
	defer c.RUnlock()
	return c.cache[txid]
}

func (c *txCache) set(txid string, status byte) {
	c.Lock()
	defer c.Unlock()

	if c.cache == nil || len(c.cache) > TxCacheSize {
		c.cache = make(map[string]byte, 256)
	}

	c.cache[txid] = status
}
