package mvcc

import "sync"

const cacheSizeMax = 8000000

type statusCache struct {
	sync.RWMutex
	min   uint64
	max   uint64
	cache map[uint64]txStatus
}

func (c *statusCache) get(txid uint64) txStatus {
	c.RLock()
	defer c.RUnlock()
	return c.cache[txid]
}

func (c *statusCache) set(txid uint64, status txStatus) {
	c.Lock()
	defer c.Unlock()
	c.cache[txid] = status

	if txid > c.max {
		c.max = txid
	} else if txid < c.min {
		c.min = txid
	}

	if len(c.cache) > cacheSizeMax {
		delim := c.min + 0.25*float64(c.max-c.min)

		for key := range c.cache {
			delete(c.cache, key)

			if key > delim {
				break
			}
		}
	}
}
