package fdbx

import (
	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Recheck - for index selection recheck
type Recheck func([]byte) (bool, error)

type v610agg struct {
	fdb *v610Conn
}

func (agg *v610agg) Intersect(
	rtp []RecordType,
	lim uint64,
	chk Recheck,
) (res []Record, err error) {
	var cnt uint8
	var rec Record
	var ids []string
	var rid map[string]uint8

	if cnt = uint8(len(rtp)); cnt == 0 {
		return nil, nil
	}

	for i := range rtp {

		if _, err = agg.fdb.fdb.ReadTransact(func(rtx fdb.ReadTransaction) (interface{}, error) {
			rng := fdb.KeyRange{Begin: fdbKey(agg.fdb.db, rtp[i].ID), End: fdbKey(agg.fdb.db, rtp[i].ID, tail)}
			opt := fdb.RangeOptions{Mode: fdb.StreamingModeSerial}
			ids = getRangeIDs(rtx, rng, opt)
			return nil, nil
		}); err != nil {
			return
		}

		if i == 0 {
			rid = make(map[string]uint8, len(ids))
			for j := range ids {
				rid[ids[j]] = 1
			}
			continue
		}

		for j := range ids {
			if _, ok := rid[ids[j]]; ok {
				rid[ids[j]]++
			}
		}
	}

	fab := rtp[0].New
	res = make([]Record, 0, len(rid))

	for id, sum := range rid {
		if sum != cnt {
			continue
		}

		if rec, err = fab(id); err != nil {
			return
		}

		res = append(res, rec)
	}

	return res, nil
}
