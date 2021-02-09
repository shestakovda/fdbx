package fdbx_test

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

const predict = "predict"
const unpredictable = "unpredictable"

// TestInterface - внешние тесты библиотеки
func TestInterface(t *testing.T) {
	suite.Run(t, new(InterfaceSuite))
}

type InterfaceSuite struct {
	suite.Suite
}

//
//func (s *InterfaceSuite) TestKey() {
//
//	// read-only
//	s.Equal(predict, fdbx.String2Key(unpredictable).LSkip(2).RSkip(4).String())
//	s.Equal(predict, fdbx.String2Key(unpredictable).RSkip(4).LSkip(2).String())
//
//	// write-only
//	s.Equal(unpredictable, fdbx.String2Key(predict).LPart('u', 'n').RPart('a', 'b', 'l', 'e').String())
//	s.Equal(unpredictable, fdbx.String2Key(predict).RPart('a', 'b', 'l', 'e').LPart('u', 'n').String())
//	s.Equal(predict, fdbx.Bytes2Key(nil).
//		LPart('d').
//		RPart('i').
//		LPart('e').
//		RPart('c').
//		LPart('r').
//		RPart('t').
//		LPart('p').
//		String(),
//	)
//
//	// read/write
//	s.Equal(unpredictable, fdbx.String2Key(unpredictable).
//		LPart('u', 'n').
//		LSkip(2).
//		RPart('a', 'b', 'l', 'e').
//		RSkip(4).
//		String(),
//	)
//	s.Equal(unpredictable, fdbx.String2Key(unpredictable).
//		RSkip(4).
//		RPart('a', 'b', 'l', 'e').
//		LSkip(2).
//		LPart('u', 'n').
//		String(),
//	)
//
//	// empty
//	key := fdbx.Bytes2Key(nil)
//	s.Equal("", key.String())
//	s.Equal("", key.LSkip(3).RSkip(4).String())
//	s.Equal("", key.RSkip(4).LSkip(3).String())
//	s.Equal("\\xab\\xcd", key.LPart(0xAB).RPart(0xCD).Printable())
//	s.Equal("\\xab\\xcd", key.RPart(0xCD).LPart(0xAB).Printable())
//	s.Equal("\xab\xcd", key.LPart(0xAB).RPart(0xCD).String())
//	s.Equal("\xab\xcd", key.RPart(0xCD).LPart(0xAB).String())
//	s.Equal("\xef", key.LSkip(0).RPart(0xEF).String())
//	s.Equal("\xef", key.RPart(0xEF).LSkip(0).String())
//	s.Equal("\xef", key.RSkip(0).LPart(0xEF).String())
//	s.Equal("\xef", key.LPart(0xEF).RSkip(0).String())
//	s.Equal("", key.LPart(0xEF).RSkip(3).String())
//	s.Equal("", key.RPart(0xEF).LSkip(3).String())
//	s.Equal("", key.LPart(0xAB).RPart(0xCD).LSkip(2).RSkip(1).String())
//	s.Equal("", key.RPart(0xCD).RSkip(1).LPart(0xAB).LSkip(2).String())
//
//	// pointer
//	word := []byte("word")
//	s.Equal("or", fdbx.Bytes2Key(word).LSkip(1).RSkip(1).String())
//	s.Equal("word", fdbx.Bytes2Key(word).String())
//	s.Equal("word", string(fdbx.Bytes2Key(word).Bytes()))
//	s.Equal("word", string(word))
//
//	// Prints
//	key = fdbx.String2Key("пекарь")
//	s.Equal("пекарь", key.String())
//	s.Equal("\\xd0\\xbf\\xd0\\xb5\\xd0\\xba\\xd0\\xb0\\xd1\\x80\\xd1\\x8c", key.Printable())
//}

// func (s *InterfaceSuite) TestPair() {

// 	kw1 := func(k fdbx.Key) (fdbx.Key, error) {
// 		return k.LSkip(2), nil
// 	}

// 	kw2 := func(k fdbx.Key) (fdbx.Key, error) {
// 		return k.RSkip(4), nil
// 	}

// 	vw1 := func(v []byte) ([]byte, error) {
// 		return append([]byte{'u', 'n'}, v...), nil
// 	}

// 	vw2 := func(v []byte) ([]byte, error) {
// 		return append(v, 'a', 'b', 'l', 'e'), nil
// 	}

// 	pair := fdbx.NewPair(fdbx.String2Key(unpredictable), []byte(predict))

// 	if key, err := pair.Key(); s.NoError(err) {
// 		s.Equal(unpredictable, key.String())
// 	}

// 	if val, err := pair.Value(); s.NoError(err) {
// 		s.Equal(predict, string(val))
// 	}

// 	pair = pair.WrapKey(kw1).WrapKey(kw2).WrapValue(vw1).WrapValue(vw2)

// 	if key, err := pair.Key(); s.NoError(err) {
// 		s.Equal(predict, key.String())
// 	}

// 	if val, err := pair.Value(); s.NoError(err) {
// 		s.Equal(unpredictable, string(val))
// 	}

// 	ew1 := func(k fdbx.Key) (fdbx.Key, error) {
// 		return k, errx.ErrForbidden
// 	}

// 	ew2 := func(v []byte) ([]byte, error) {
// 		return v, errx.ErrNotFound
// 	}

// 	if key, err := pair.WrapKey(ew1).Key(); s.Error(err) {
// 		s.True(errx.Is(err, errx.ErrForbidden))
// 		s.Nil(key)
// 	}

// 	if val, err := pair.WrapValue(ew2).Value(); s.Error(err) {
// 		s.True(errx.Is(err, errx.ErrNotFound))
// 		s.Nil(val)
// 	}
// }
//
//func BenchmarkKey(b *testing.B) {
//	b.Run("read-only", func(br *testing.B) {
//		for i := 0; i < br.N; i++ {
//			require.Equal(br, predict, fdbx.String2Key(unpredictable).LSkip(2).RSkip(4).String())
//		}
//	})
//
//	b.Run("write-only", func(bw *testing.B) {
//		for i := 0; i < bw.N; i++ {
//			require.Equal(bw, unpredictable, fdbx.String2Key(predict).
//				LPart('u', 'n').
//				RPart('a', 'b', 'l', 'e').
//				String(),
//			)
//		}
//	})
//
//	b.Run("read-write", func(bwr *testing.B) {
//		for i := 0; i < bwr.N; i++ {
//			require.Equal(bwr, unpredictable, fdbx.String2Key(unpredictable).
//				LPart('u', 'n').
//				LSkip(2).
//				RPart('a', 'b', 'l', 'e').
//				RSkip(4).
//				String(),
//			)
//		}
//	})
//
//	b.Run("many-write", func(mw *testing.B) {
//		for i := 0; i < mw.N; i++ {
//			require.Equal(mw, predict, fdbx.Bytes2Key(nil).
//				LPart('d').
//				RPart('i').
//				LPart('e').
//				RPart('c').
//				LPart('r').
//				RPart('t').
//				LPart('p').
//				String(),
//			)
//		}
//	})
//}
