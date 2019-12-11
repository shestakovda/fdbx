package fdbx

import (
	"context"
	"time"
)

func newMockQueue(conn *MockConn, qtype uint16, f Fabric, prefix []byte) (*mockQueue, error) {
	return &mockQueue{MockConn: conn, id: qtype, mf: f, pf: prefix}, nil
}

type mockQueue struct {
	*MockConn
	id uint16
	pf []byte
	mf Fabric
}

func (q *mockQueue) Ack(db DB, id []byte) error                            { return q.FAck(db, id) }
func (q *mockQueue) Pub(db DB, id []byte, t time.Time) error               { return q.FPub(db, id, t) }
func (q *mockQueue) Sub(ctx context.Context) (<-chan Record, <-chan error) { return q.FSub(ctx) }
func (q *mockQueue) SubOne(ctx context.Context) (Record, error)            { return q.FSubOne(ctx) }
func (q *mockQueue) SubList(ctx context.Context, limit uint) ([]Record, error) {
	return q.FSubList(ctx, limit)
}
func (q *mockQueue) GetLost(limit uint) ([]Record, error) { return q.FGetLost(limit) }
func (q *mockQueue) Settings() (uint16, Fabric)           { return q.id, q.mf }
