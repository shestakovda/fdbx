package mvcc

import "sync"

const cacheSizeMax = 8000000

func newStatusCache() *statusCache {
	return &statusCache{
		cache: make(map[uint64]byte, 64),
	}
}

type statusCache struct {
	sync.RWMutex
	min   uint64
	max   uint64
	cache map[uint64]byte
}

func (c *statusCache) get(txid uint64) byte {
	c.RLock()
	defer c.RUnlock()
	return c.cache[txid]
}

func (c *statusCache) set(txid uint64, status byte) {
	c.Lock()
	defer c.Unlock()
	c.cache[txid] = status

	if txid > c.max {
		c.max = txid
	} else if txid < c.min || c.min == 0 {
		c.min = txid
	}

	if len(c.cache) > cacheSizeMax {
		delim := c.min + uint64(0.25*float64(c.max-c.min))

		for key := range c.cache {
			if key < delim {
				delete(c.cache, key)
			}
		}
	}
}
