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
		TestQueue1, func(p fdbx.Pair) ([]byte, error) {
			if val, err := p.Value(); s.NoError(err) {
				s.Equal(msg1, string(val))
			}
			return []byte(msg2), nil
		},
		rpc.Refresh(20*time.Millisecond),
		rpc.Sync(),
	))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	srv.Run(ctx, s.cn)

	if res, err := cli.SyncExec(ctx, TestQueue1, []byte(msg1)); s.NoError(err) {
		s.Equal(msg2, string(res))
	}

	srv.Stop()
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
		TestQueue1, func(p fdbx.Pair) ([]byte, error) {
			watch["OnTask1"]++
			if key, err := p.Key(); s.NoError(err) {
				s.Equal(id1, key)
			}
			if val, err := p.Value(); s.NoError(err) {
				s.Equal("msg1", string(val))
			}
			return nil, nil
		},
		rpc.Refresh(20*time.Millisecond),
		rpc.OnError(func(e error) (bool, time.Duration) {
			watch["OnError1"]++
			s.NoError(e)
			return false, 0
		}),
		rpc.OnListenError(func(e error) (bool, time.Duration) {
			watch["OnListen1"]++
			s.NoError(e)
			return false, 0
		}),
	))

	s.Require().NoError(srv.Endpoint(
		TestQueue2, func(p fdbx.Pair) ([]byte, error) {
			watch["OnTask2"]++
			if key, err := p.Key(); s.NoError(err) {
				s.Equal(id3, key)
			}
			if val, err := p.Value(); s.NoError(err) {
				if string(val) == "msg3" {
					watch["OnTask2_1"]++
					return nil, wtf.WithStack()
				} else {
					watch["OnTask2_2"]++
					s.Equal("updmsg", string(val))
				}
			}
			return nil, nil
		},
		rpc.Refresh(20*time.Millisecond),
		rpc.OnError(func(e error) (bool, time.Duration) {
			watch["OnError2"]++
			if s.Error(e) {
				s.True(errx.Is(e, wtf))

				// Обновляем значение в качестве исправления ошибки
				tx, err := mvcc.Begin(s.cn)
				s.Require().NoError(err)
				s.Require().NoError(s.tbl.Upsert(tx, fdbx.NewPair(id3, []byte("updmsg"))))
				s.Require().NoError(tx.Commit())
				return true, 100 * time.Millisecond
			}
			return false, 0
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
	s.Require().NoError(orm.NewQueue(TestQueue1, s.tbl).Pub(tx, time.Now().Add(80*time.Millisecond), id1))
	s.Require().NoError(orm.NewQueue(TestQueue2, s.tbl).Pub(tx, time.Now(), id3))
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
