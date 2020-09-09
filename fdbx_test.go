package fdbx_test

import (
	"testing"

	"github.com/shestakovda/fdbx"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// TestInterface - внешние тесты библиотеки
func TestInterface(t *testing.T) {
	suite.Run(t, new(InterfaceSuite))
}

type InterfaceSuite struct {
	suite.Suite
}

func (s *InterfaceSuite) TestKey() {
	const predict = "predict"
	const unpredictable = "unpredictable"

	// read-only
	s.Equal(predict, fdbx.Key(unpredictable).LSkip(2).RSkip(4).String())
	s.Equal(predict, fdbx.Key(unpredictable).RSkip(4).LSkip(2).String())

	// write-only
	s.Equal(unpredictable, fdbx.Key(predict).LPart('u', 'n').RPart('a', 'b', 'l', 'e').String())
	s.Equal(unpredictable, fdbx.Key(predict).RPart('a', 'b', 'l', 'e').LPart('u', 'n').String())
	s.Equal(predict, fdbx.Key(nil).
		LPart('d').
		RPart('i').
		LPart('e').
		RPart('c').
		LPart('r').
		RPart('t').
		LPart('p').
		String(),
	)

	// read/write
	s.Equal(unpredictable, fdbx.Key(unpredictable).
		LPart('u', 'n').
		LSkip(2).
		RPart('a', 'b', 'l', 'e').
		RSkip(4).
		String(),
	)
	s.Equal(unpredictable, fdbx.Key(unpredictable).
		RSkip(4).
		RPart('a', 'b', 'l', 'e').
		LSkip(2).
		LPart('u', 'n').
		String(),
	)

	// empty
	key := fdbx.Key(nil)
	s.Equal("", key.String())
	s.Equal("", key.LSkip(3).RSkip(4).String())
	s.Equal("", key.RSkip(4).LSkip(3).String())
	s.Equal("\\xab\\xcd", key.LPart(0xAB).RPart(0xCD).String())
	s.Equal("\\xab\\xcd", key.RPart(0xCD).LPart(0xAB).String())
	s.Equal("\\xef", key.LSkip(0).RPart(0xEF).String())
	s.Equal("\\xef", key.RPart(0xEF).LSkip(0).String())
	s.Equal("\\xef", key.RSkip(0).LPart(0xEF).String())
	s.Equal("\\xef", key.LPart(0xEF).RSkip(0).String())
	s.Equal("", key.LPart(0xEF).RSkip(3).String())
	s.Equal("", key.RPart(0xEF).LSkip(3).String())
	s.Equal("", key.LPart(0xAB).RPart(0xCD).LSkip(2).RSkip(1).String())
	s.Equal("", key.RPart(0xCD).RSkip(1).LPart(0xAB).LSkip(2).String())

	// pointer
	word := []byte("word")
	s.Equal("or", fdbx.Key(word).LSkip(1).RSkip(1).String())
	s.Equal("word", fdbx.Key(word).String())
	s.Equal("word", string(fdbx.Key(word).Bytes()))
	s.Equal("word", string(word))
}

func BenchmarkKey(b *testing.B) {
	const predict = "predict"
	const unpredictable = "unpredictable"

	b.Run("read-only", func(br *testing.B) {
		buf := []byte(unpredictable)
		for i := 0; i < br.N; i++ {
			require.Equal(br, predict, fdbx.Key(buf).LSkip(2).RSkip(4).String())
		}
	})

	b.Run("write-only", func(bw *testing.B) {
		buf := []byte(predict)
		for i := 0; i < bw.N; i++ {
			require.Equal(bw, unpredictable, fdbx.Key(buf).LPart('u', 'n').RPart('a', 'b', 'l', 'e').String())
		}
	})

	b.Run("read-write", func(bwr *testing.B) {
		buf := []byte(unpredictable)
		for i := 0; i < bwr.N; i++ {
			require.Equal(bwr, unpredictable, fdbx.Key(buf).
				LPart('u', 'n').
				LSkip(2).
				RPart('a', 'b', 'l', 'e').
				RSkip(4).
				String(),
			)
		}
	})

	b.Run("many-write", func(mw *testing.B) {
		for i := 0; i < mw.N; i++ {
			require.Equal(mw, predict, fdbx.Key(nil).
				LPart('d').
				RPart('i').
				LPart('e').
				RPart('c').
				LPart('r').
				RPart('t').
				LPart('p').
				String(),
			)
		}
	})
}
