package rpc

import (
	"time"

	"github.com/golang/glog"
)

func getOpts(args []Option) (o options) {
	o.vpack = 1000
	o.vwait = time.Hour
	o.refresh = time.Second
	o.onError = defOnError
	o.onListen = defOnError

	for i := range args {
		args[i](&o)
	}
	return
}

type options struct {
	asRPC bool

	onError  ErrorHandler
	onListen ErrorHandler

	refresh time.Duration

	vpack uint64
	vwait time.Duration
}

func Sync() Option {
	return func(o *options) {
		o.asRPC = true
	}
}

func VacuumPack(d int) Option {
	return func(o *options) {
		if d > 0 {
			o.vpack = uint64(d)
		}
	}
}

func VacuumWait(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.vwait = d
		}
	}
}

func Refresh(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.refresh = d
		}
	}
}

func OnError(h ErrorHandler) Option {
	return func(o *options) {
		if h != nil {
			o.onError = h
		}
	}
}

func OnListenError(h ErrorHandler) Option {
	return func(o *options) {
		if h != nil {
			o.onListen = h
		}
	}
}

func defOnError(err error) (bool, time.Duration) {
	glog.Errorf("%+v", err)
	return false, 0
}