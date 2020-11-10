package db_test

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/stretchr/testify/suite"
)

const TestDB byte = 0x12

// TestInterface - внешние тесты библиотеки
func TestInterface(t *testing.T) {
	suite.Run(t, new(InterfaceSuite))
}

type InterfaceSuite struct {
	suite.Suite
}

func (s *InterfaceSuite) TestConnection() {
	if _, err := db.ConnectV610(0xFF); s.Error(err) {
		s.True(errx.Is(err, db.ErrConnect))
	}

	cn, err := db.ConnectV610(
		TestDB,
		db.ClusterFile(""),
	)
	s.Require().NoError(err)
	s.Require().NoError(cn.Clear())
	s.Equal(TestDB, cn.DB())

	var val []byte
	var buf [8]byte
	var waiter fdbx.Waiter
	var waiter2 fdbx.Waiter

	const num int64 = 123
	const add int64 = -100
	binary.LittleEndian.PutUint64(buf[:], uint64(num))

	key1 := fdbx.Key("key1")
	key2 := fdbx.Key("key2")
	key3 := fdbx.Key("key3")

	s.Require().NoError(cn.Write(func(w db.Writer) (exp error) {
		if val, exp = w.Data(key1).Value(); exp != nil {
			return
		}
		s.Empty(val)

		if exp = w.Upsert(fdbx.NewPair(key1, []byte("val1"))); exp != nil {
			return
		}

		if exp = w.Upsert(fdbx.NewPair(key2, buf[:])); exp != nil {
			return
		}
		w.Versioned(key3)
		waiter = w.Watch(key2)
		waiter2 = w.Watch(key3)

		if val, exp = w.Data(key1).Value(); exp != nil {
			return
		}
		s.Equal("val1", string(val))
		return nil
	}))

	wg := new(sync.WaitGroup)
	wg.Add(1)
	go func() {
		defer wg.Done()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		time.Sleep(time.Millisecond)

		if err := waiter2.Resolve(ctx); s.Error(err) {
			s.True(errx.Is(err, db.ErrWait))
		}

		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond)
		defer cancel()

		if err := waiter2.Resolve(ctx); s.Error(err) {
			s.True(errx.Is(err, db.ErrWait))
		}

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.NoError(waiter.Resolve(ctx))

		// Проверяем, что к этому моменту уже всё изменилось
		s.Require().NoError(cn.Read(func(r db.Reader) error {

			if val, err = r.Data(key1).Value(); s.NoError(err) {
				s.Equal("val2", string(val))
			}

			if val, err = r.Data(key2).Value(); s.NoError(err) {
				s.Equal(num+add, int64(binary.LittleEndian.Uint64(val)))
			}

			if val, err = r.Data(key3).Value(); s.NoError(err) {
				s.Empty(val)
			}

			s.Len(r.List(nil, nil, 0, false).Resolve(), 2)
			return nil
		}))

	}()

	s.Require().NoError(cn.Read(func(r db.Reader) error {

		if val, err = r.Data(key1).Value(); s.NoError(err) {
			s.Equal("val1", string(val))
		}

		if val, err = r.Data(key2).Value(); s.NoError(err) {
			s.Equal(num, int64(binary.LittleEndian.Uint64(val)))
		}

		if val, err = r.Data(key3).Value(); s.NoError(err) {
			s.Len(val, 10)
		}

		return nil
	}))

	s.Require().NoError(cn.Write(func(w db.Writer) error {
		w.Upsert(fdbx.NewPair(key1, []byte("val2")))
		w.Increment(key2, add)
		w.Delete(key3)
		return nil
	}))

	wg.Wait()

	s.Require().NoError(cn.Write(func(w db.Writer) error {
		w.Lock(key1, key3)
		w.Erase(key1, key3)
		s.Len(w.List(nil, nil, 0, false).Resolve(), 0)
		return nil
	}))
}
