package orm_test

import (
	"context"
	"testing"
	"time"

	"github.com/shestakovda/fdbx/db"
	"github.com/shestakovda/fdbx/mvcc"
	"github.com/shestakovda/fdbx/orm"
	"github.com/stretchr/testify/suite"
)

const TestDB byte = 0x10
const TestTable uint16 = 1

func TestORM(t *testing.T) {
	suite.Run(t, new(ORMSuite))
}

type ORMSuite struct {
	suite.Suite

	tx mvcc.Tx
	cn db.Connection
}

func (s *ORMSuite) SetupTest() {
	var err error

	s.cn, err = db.ConnectV610(TestDB)
	s.Require().NoError(err)
	s.Require().NoError(s.cn.Clear())

	s.tx, err = mvcc.Begin(s.cn)
	s.Require().NoError(err)
}

func (s *ORMSuite) TearDownTest() {
	s.NoError(s.tx.Cancel())
}

func (s *ORMSuite) TestWorkflow() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	tbl := orm.Table(TestTable, newModel)

	s.Require().NoError(tbl.Upsert(s.tx, &simpleModel{ID: "id1", Msg: "msg1"}))
	s.Require().NoError(tbl.Upsert(s.tx, &simpleModel{ID: "id2", Msg: "msg2"}))
	s.Require().NoError(tbl.Upsert(s.tx, &simpleModel{ID: "id3", Msg: "msg3"}))

	if list, err := tbl.Select(s.tx).All(ctx); s.NoError(err) {
		s.Len(list, 3)

		s.Equal("id1", list[0].(*simpleModel).ID)
		s.Equal("msg1", list[0].(*simpleModel).Msg)

		s.Equal("id2", list[1].(*simpleModel).ID)
		s.Equal("msg2", list[1].(*simpleModel).Msg)

		s.Equal("id3", list[2].(*simpleModel).ID)
		s.Equal("msg3", list[2].(*simpleModel).Msg)
	}

	s.Require().NoError(tbl.Select(s.tx).Delete(ctx))

	if list, err := tbl.Select(s.tx).All(ctx); s.NoError(err) {
		s.Empty(list)
	}
}

func newModel(key mvcc.Key) orm.Model {
	return &simpleModel{
		ID: key.String(),
	}
}

type simpleModel struct {
	ID  string
	Msg string
}

func (m simpleModel) Key() mvcc.Key              { return mvcc.NewStrKey(m.ID) }
func (m simpleModel) Pack() (mvcc.Value, error)  { return mvcc.NewStrValue(m.Msg), nil }
func (m *simpleModel) Unpack(v mvcc.Value) error { m.Msg = v.String(); return nil }
