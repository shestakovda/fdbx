package fdbx

import (
	"context"
	"sync"
)

func Intersect(
	ctx context.Context,
	crs []CursorID,
	lim uint64,
) (res []string, err error) {
	var max uint64

	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	idscl := make([]<-chan string, len(crs))
	errcl := make([]<-chan error, len(crs))

	for i := range crs {
		max |= 1 << i
		idsc, errc := crs[i].Select(wctx)
		idscl = append(idscl, idsc)
		errcl = append(errcl, errc)
	}

	sum := make(map[string]uint64, 64)
	cum := make(map[string]struct{}, 64)

	for chid := range mergeStrc(idscl...) {
		sum[chid.id] |= 1 << chid.n

		if sum[chid.id] == max {
			cum[chid.id] = struct{}{}
		}
	}

	if err = waitErr(errcl...); err != nil {
		return
	}

	res = make([]string, 0, len(cum))

	for id := range cum {
		res = append(res, id)
	}

	return res, nil
}

type chanID struct {
	n  uint8
	id string
}

func mergeStrc(strcl ...<-chan string) <-chan *chanID {
	var wgp sync.WaitGroup

	merged := make(chan *chanID, len(strcl))

	wgp.Add(len(strcl))

	merge := func(n uint8, strc <-chan string) {
		defer wgp.Done()

		for str := range strc {
			if str != "" {
				merged <- &chanID{n, str}
				return
			}
		}
	}

	for i, strc := range strcl {
		go merge(uint8(i), strc)
	}

	go func() { wgp.Wait(); close(merged) }()

	return merged
}

func mergeErrc(errcl ...<-chan error) <-chan error {
	var wgp sync.WaitGroup

	merged := make(chan error, len(errcl))

	wgp.Add(len(errcl))

	merge := func(errc <-chan error) {
		defer wgp.Done()

		for err := range errc {
			if err != nil {
				merged <- err
				return
			}
		}
	}

	for _, errc := range errcl {
		go merge(errc)
	}

	go func() { wgp.Wait(); close(merged) }()

	return merged
}

func waitErr(errs ...<-chan error) error {
	for err := range mergeErrc(errs...) {
		if err != nil {
			return err
		}
	}

	return nil
}
