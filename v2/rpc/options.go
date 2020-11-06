package rpc

import (
	"time"

	"github.com/golang/glog"
)

func getOpts(args []Option) (o options) {
	o.vpack = 1000
	o.vwait = time.Hour
	o.refresh = time.Second
	o.timeout = time.Minute
	o.onListen = defOnListen

	for i := range args {
		args[i](&o)
	}
	return
}

type options struct {
	async bool

	onError  ErrorHandler
	onListen ListenHandler

	refresh time.Duration
	timeout time.Duration

	vpack uint64
	vwait time.Duration
}

func Async() Option {
	return func(o *options) {
		o.async = true
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

func Timeout(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.timeout = d
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

func OnListenError(h ListenHandler) Option {
	return func(o *options) {
		if h != nil {
			o.onListen = h
		}
	}
}

func defOnListen(err error) (bool, time.Duration) {
	glog.Errorf("%+v", err)
	return false, 0
}
