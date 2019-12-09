package fdbx

import (
	"context"
	"time"
)

func newMockQueue(conn *MockConn, qtype uint16, f Fabric) (*mockQueue, error) {
	return &mockQueue{MockConn: conn, id: qtype, mf: f}, nil
}

type mockQueue struct {
	*MockConn
	id uint16
	mf Fabric
}

func (q *mockQueue) Ack(db DB, m Record) error                             { return q.FAck(db, m) }
func (q *mockQueue) Pub(db DB, m Record, t time.Time) error                { return q.FPub(db, m, t) }
func (q *mockQueue) Sub(ctx context.Context) (<-chan Record, <-chan error) { return q.FSub(ctx) }
func (q *mockQueue) SubOne(ctx context.Context) (Record, error)            { return q.FSubOne(ctx) }
func (q *mockQueue) SubList(ctx context.Context, limit int) ([]Record, error) {
	return q.FSubList(ctx, limit)
}
func (q *mockQueue) GetLost(limit int) ([]Record, error) { return q.FGetLost(limit) }
func (q *mockQueue) Settings() (uint16, Fabric)          { return q.id, q.mf }
