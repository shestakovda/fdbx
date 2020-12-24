package fdbx

import (
	"context"
	"encoding/binary"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	fbs "github.com/google/flatbuffers/go"
	"github.com/shestakovda/errx"
)

var fbsPool = sync.Pool{New: func() interface{} { return fbs.NewBuilder(128) }}

// ExitSignals - список системных сигналов, которые означают завершение программы
//nolint:gochecknoglobals
var ExitSignals = []os.Signal{
	syscall.SIGINT,
	syscall.SIGHUP,
	syscall.SIGABRT,
	syscall.SIGQUIT,
	syscall.SIGTERM,
}

// Time2Byte - преобразователь времени в массив байт
func Time2Byte(t time.Time) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(t.UTC().UnixNano()))
	return buf[:]
}

// Byte2Time - преобразователь времени в массив байт
func Byte2Time(buf []byte) (time.Time, error) {
	if len(buf) < 8 {
		return time.Time{}, ErrByte2Time.WithDebug(errx.Debug{
			"buf": buf,
		})
	}

	return time.Unix(0, int64(binary.BigEndian.Uint64(buf[:8]))).UTC(), nil
}

func FlatPack(obj FlatPacker) []byte {
	buf := fbsPool.Get().(*fbs.Builder)
	buf.Reset()
	buf.Finish(obj.Pack(buf))
	tmp := buf.FinishedBytes()
	res := make([]byte, len(tmp))
	copy(res, tmp)
	fbsPool.Put(buf)
	return res
}

func WithSignal(ctx context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
	exit := make(chan os.Signal, 1)
	signal.Notify(exit, signals...)

	wctx, cancel := context.WithCancel(ctx)

	go func() {
		defer signal.Stop(exit)
		defer cancel()

		for {
			select {
			case <-wctx.Done():
				return
			case <-exit:
				return
			}
		}
	}()

	return wctx, cancel
}
