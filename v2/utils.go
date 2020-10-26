package fdbx

import (
	"encoding/binary"
	"time"

	"github.com/shestakovda/errx"
)

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

	return time.Unix(0, int64(binary.BigEndian.Uint64(buf[:8]))), nil
}
