package db_test

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/apple/foundationdb/bindings/go/src/fdb"

	"github.com/shestakovda/errx"
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
	if _, err := db.Connect(0xFF); s.Error(err) {
		s.True(errx.Is(err, db.ErrConnect))
	}

	cn, err := db.Connect(
		TestDB,
		db.ClusterFile(""),
	)
	s.Require().NoError(err)
	s.Require().NoError(cn.Clear())
	s.Equal(TestDB, cn.ID)

	var buf [8]byte
	var waiter db.Waiter
	var waiter2 db.Waiter

	const num int64 = 123
	const add int64 = -100
	binary.LittleEndian.PutUint64(buf[:], uint64(num))

	key1 := fdb.Key("key1")
	key2 := fdb.Key("key2")
	key3 := fdb.Key("key3")

	s.Require().NoError(cn.Write(func(w db.Writer) error {
		s.Empty(w.Data(key1))

		w.Upsert(fdb.KeyValue{Key: key1, Value: []byte("val1")})
		w.Upsert(fdb.KeyValue{Key: key2, Value: buf[:]})
		w.Increment(key3, num)

		waiter = w.Watch(key2)
		waiter2 = w.Watch(key3)

		s.Equal("val1", string(w.Data(key1)))
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

		s.NoError(waiter2.Resolve(ctx))

		ctx, cancel = context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		s.NoError(waiter.Resolve(ctx))

		// Проверяем, что к этому моменту уже всё изменилось
		s.Require().NoError(cn.Read(func(r db.Reader) error {
			s.Equal("val2", string(r.Data(key1)))
			s.Equal(num+add, int64(binary.LittleEndian.Uint64(r.Data(key2))))
			s.Empty(r.Data(key3))
			s.Len(r.List(nil, nil, 0, false, false).GetSliceOrPanic(), 2)
			return nil
		}))

	}()

	s.Require().NoError(cn.Read(func(r db.Reader) error {
		s.Equal("val1", string(r.Data(key1)))
		s.Equal(num, int64(binary.LittleEndian.Uint64(r.Data(key2))))
		s.Equal(num, int64(binary.LittleEndian.Uint64(r.Data(key3))))
		return nil
	}))

	s.Require().NoError(cn.Write(func(w db.Writer) error {
		w.Upsert(fdb.KeyValue{Key: key1, Value: []byte("val2")})
		w.Increment(key2, add)
		w.Delete(key3)
		return nil
	}))

	wg.Wait()

	s.Require().NoError(cn.Write(func(w db.Writer) error {
		w.Lock(key1, key3)
		w.Erase(key1, key3)
		s.Len(w.List(nil, nil, 0, false, false).GetSliceOrPanic(), 0)
		return nil
	}))
}
