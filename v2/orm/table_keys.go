package orm

import "github.com/shestakovda/fdbx/v2"

func newTableKeyManager(id uint16) fdbx.KeyManager {
	m := tableKeyManager{
		id: id,
	}

	return &m
}

type tableKeyManager struct {
	id uint16
}

func (m tableKeyManager) Wrap(k fdbx.Key) fdbx.Key {
	return k.LPart(byte(m.id>>8), byte(m.id), nsData)
}

func (m tableKeyManager) Unwrap(k fdbx.Key) fdbx.Key {
	return k.LSkip(3)
}

func (m tableKeyManager) Wrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Wrap(k), nil
}

func (m tableKeyManager) Unwrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Unwrap(k), nil
}

func newBLOBKeyManager(id uint16) fdbx.KeyManager {
	m := blobKeyManager{
		id: id,
	}

	return &m
}

type blobKeyManager struct {
	id uint16
}

func (m blobKeyManager) Wrap(k fdbx.Key) fdbx.Key {
	return k.LPart(byte(m.id>>8), byte(m.id), nsBLOB)
}

func (m blobKeyManager) Unwrap(k fdbx.Key) fdbx.Key {
	return k.LSkip(3)
}

func (m blobKeyManager) Wrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Wrap(k), nil
}

func (m blobKeyManager) Unwrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Unwrap(k), nil
}

func newIndexKeyManager(tbl, idx uint16) fdbx.KeyManager {
	m := indexKeyManager{
		tbl: tbl,
		idx: idx,
	}

	return &m
}

type indexKeyManager struct {
	tbl uint16
	idx uint16
}

func (m indexKeyManager) Wrap(k fdbx.Key) fdbx.Key {
	return k.LPart(byte(m.tbl>>8), byte(m.tbl), nsIndex, byte(m.idx>>8), byte(m.idx))
}

func (m indexKeyManager) Unwrap(k fdbx.Key) fdbx.Key {
	return k.LSkip(5)
}

func (m indexKeyManager) Wrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Wrap(k), nil
}

func (m indexKeyManager) Unwrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Unwrap(k), nil
}

func newQueueKeyManager(tbl, queue uint16, prefix []byte) fdbx.KeyManager {
	m := queueKeyManager{
		tbl:    tbl,
		queue:  queue,
		prefix: prefix,
	}

	return &m
}

type queueKeyManager struct {
	tbl    uint16
	queue  uint16
	prefix []byte
}

func (m queueKeyManager) Wrap(k fdbx.Key) fdbx.Key {
	if len(m.prefix) > 0 {
		k = k.LPart(m.prefix...)
	}

	return k.LPart(byte(m.tbl>>8), byte(m.tbl), nsQueue, byte(m.queue>>8), byte(m.queue))
}

func (m queueKeyManager) Unwrap(k fdbx.Key) fdbx.Key {
	return k.LSkip(5 + 1 + 8 + uint16(len(m.prefix)))
}

func (m queueKeyManager) Wrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Wrap(k), nil
}

func (m queueKeyManager) Unwrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Unwrap(k), nil
}

func NewWatchKeyManager(id uint16) fdbx.KeyManager {
	m := watchKeyManager{
		id: id,
	}

	return &m
}

type watchKeyManager struct {
	id uint16
}

func (m watchKeyManager) Wrap(k fdbx.Key) fdbx.Key {
	return k.LPart(byte(m.id>>8), byte(m.id), nsWatch)
}

func (m watchKeyManager) Unwrap(k fdbx.Key) fdbx.Key {
	return k.LSkip(3)
}

func (m watchKeyManager) Wrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Wrap(k), nil
}

func (m watchKeyManager) Unwrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Unwrap(k), nil
}

func NewQueryKeyManager(id uint16) fdbx.KeyManager {
	m := queryKeyManager{
		id: id,
	}

	return &m
}

type queryKeyManager struct {
	id uint16
}

func (m queryKeyManager) Wrap(k fdbx.Key) fdbx.Key {
	return k.LPart(byte(m.id>>8), byte(m.id), nsQuery)
}

func (m queryKeyManager) Unwrap(k fdbx.Key) fdbx.Key {
	return k.LSkip(3)
}

func (m queryKeyManager) Wrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Wrap(k), nil
}

func (m queryKeyManager) Unwrapper(k fdbx.Key) (fdbx.Key, error) {
	return m.Unwrap(k), nil
}
