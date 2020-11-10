package rpc_test

import (
	"context"
	"testing"
	"time"

	"github.com/shestakovda/errx"
	"github.com/shestakovda/fdbx/v2"
	"github.com/shestakovda/fdbx/v2/db"
	"github.com/shestakovda/fdbx/v2/mvcc"
	"github.com/shestakovda/fdbx/v2/orm"
	"github.com/shestakovda/fdbx/v2/rpc"
	"github.com/shestakovda/typex"
	"github.com/stretchr/testify/suite"
)

const TestDB byte = 0x10
const TestTable uint16 = 1
const TestQueue1 uint16 = 2
const TestQueue2 uint16 = 3

func TestRPC(t *testing.T) {
	suite.Run(t, new(RPCSuite))
}

type RPCSuite struct {
	suite.Suite

	cn  db.Connection
	tbl orm.Table
}

func (s *RPCSuite) SetupTest() {
	var err error

	s.cn, err = db.ConnectV610(TestDB)
	s.Require().NoError(err)
	s.Require().NoError(s.cn.Clear())

	s.tbl = orm.NewTable(TestTable)
}

func (s *RPCSuite) TestSyncRPC() {
	const msg1, msg2 = "msg1", "msg2"

	srv := rpc.NewServer(TestTable)
	cli := rpc.NewClient(s.cn, TestTable)

	s.Require().NoError(srv.Endpoint(
		TestQueue1, func(t orm.Task) ([]byte, error) {
			s.Equal(msg1, string(t.Body()))
			return []byte(msg2), nil
		},
		rpc.Refresh(20*time.Millisecond),
	))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv.Run(ctx, s.cn, rpc.VacuumWait(20*time.Millisecond))

	if res, err := cli.Call(ctx, TestQueue1, []byte(msg1)); s.NoError(err) {
		s.Equal(msg2, string(res))
	}

	// Добавляем левые ключи для теста автовакуума

	wuid := fdbx.Key(typex.NewUUID())
	wkey := mvcc.NewTxKeyManager().Wrap(orm.NewWatchKeyManager(TestTable).Wrap(wuid))
	wnow := fdbx.NewPair(wkey, fdbx.Time2Byte(time.Now()))

	wuid2 := fdbx.Key(typex.NewUUID())
	wkey2 := mvcc.NewTxKeyManager().Wrap(orm.NewWatchKeyManager(TestTable).Wrap(wuid2))
	wnow2 := fdbx.NewPair(wkey2, fdbx.Time2Byte(time.Now().Add(-36*time.Hour)))

	s.Require().NoError(s.cn.Write(func(w db.Writer) (exp error) {
		if exp = w.Upsert(wnow); exp != nil {
			return
		}
		if exp = w.Upsert(wnow2); exp != nil {
			return
		}
		return nil
	}))

	// Ждем, когда вакуум почистит
	time.Sleep(50 * time.Millisecond)

	// Останавливаем все службы
	srv.Stop()

	// Проверяем, что старого ключа нет, а новый еще на месте
	s.Require().NoError(s.cn.Read(func(r db.Reader) (e error) {
		wnil := mvcc.NewTxKeyManager().Wrap(orm.NewWatchKeyManager(TestTable).Wrap(nil))
		list := r.List(wnil, wnil, 1000, false)()

		if s.Len(list, 1) {
			if key, exp := list[0].Key(); s.NoError(exp) {
				s.Equal(wkey, key)
			}
		}
		return nil
	}))
}

func (s *RPCSuite) TestServer() {
	id1 := fdbx.Key("id1")
	id2 := fdbx.Key("id2")
	id3 := fdbx.Key("id3")

	tx, err := mvcc.Begin(s.cn)
	s.Require().NoError(err)
	s.Require().NoError(s.tbl.Upsert(tx,
		fdbx.NewPair(id1, []byte("msg1")),
		fdbx.NewPair(id2, []byte("msg2")),
		fdbx.NewPair(id3, []byte("msg3")),
	))
	s.Require().NoError(tx.Commit())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	wtf := errx.New("wtf")
	srv := rpc.NewServer(TestTable)
	watch := make(map[string]int, 6)

	s.Require().NoError(srv.Endpoint(
		TestQueue1, func(t orm.Task) ([]byte, error) {
			watch["OnTask1"]++
			s.Equal(id1, t.Key())
			s.Equal("msg1", string(t.Body()))
			return nil, nil
		},
		rpc.Async(),
		rpc.Refresh(20*time.Millisecond),
		rpc.OnError(func(t orm.Task, e error) (bool, time.Duration, []byte, error) {
			watch["OnError1"]++
			s.NoError(e)
			return false, 0, nil, nil
		}),
		rpc.OnListenError(func(e error) (bool, time.Duration) {
			watch["OnListen1"]++
			s.NoError(e)
			return false, 0
		}),
	))

	s.Require().NoError(srv.Endpoint(
		TestQueue2, func(t orm.Task) ([]byte, error) {
			watch["OnTask2"]++
			s.Equal(id3, t.Key())
			if string(t.Body()) == "msg3" {
				watch["OnTask2_1"]++
				return nil, wtf.WithStack()
			} else {
				watch["OnTask2_2"]++
				s.Equal("updmsg", string(t.Body()))
			}
			return nil, nil
		},
		rpc.Async(),
		rpc.Refresh(20*time.Millisecond),
		rpc.OnError(func(t orm.Task, e error) (bool, time.Duration, []byte, error) {
			watch["OnError2"]++
			if s.Error(e) {
				s.True(errx.Is(e, wtf))

				// Обновляем значение в качестве исправления ошибки
				tx, err := mvcc.Begin(s.cn)
				s.Require().NoError(err)
				s.Require().NoError(s.tbl.Upsert(tx, fdbx.NewPair(id3, []byte("updmsg"))))
				s.Require().NoError(tx.Commit())
				return true, 100 * time.Millisecond, nil, nil
			}
			return false, 0, nil, nil
		}),
		rpc.OnListenError(func(e error) (bool, time.Duration) {
			watch["OnListen2"]++
			s.NoError(e)
			return false, 0
		}),
	))

	srv.Run(ctx, s.cn)

	tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)
	s.Require().NoError(orm.NewQueue(TestQueue1, s.tbl).Pub(tx, id1, orm.Delay(80*time.Millisecond)))
	s.Require().NoError(orm.NewQueue(TestQueue2, s.tbl).Pub(tx, id3))
	s.Require().NoError(tx.Commit())

	time.Sleep(2 * time.Second)

	srv.Stop()

	s.Equal(1, watch["OnTask1"])
	s.Equal(0, watch["OnError1"])
	s.Equal(0, watch["OnListen1"])
	s.Equal(2, watch["OnTask2"])
	s.Equal(1, watch["OnTask2_1"])
	s.Equal(1, watch["OnTask2_2"])
	s.Equal(1, watch["OnError2"])
	s.Equal(0, watch["OnListen2"])
}
