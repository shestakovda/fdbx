package fdbx

import (
	fbs "github.com/google/flatbuffers/go"

	"github.com/shestakovda/errx"
)

// FlatPacker - интерфейс для упаковки flatbuffers
type FlatPacker interface {
	Pack(*fbs.Builder) fbs.UOffsetT
}

var (
	ErrKey       = errx.New("Ошибка загрузки ключа")
	ErrValue     = errx.New("Ошибка загрузки значения")
	ErrByte2Time = errx.New("Ошибка преобразования значения во время")
)
