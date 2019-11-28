package fdbx

import (
	"context"
	"time"
)

func newMockQueue(conn *MockConn, qtype uint16, f Fabric) *mockQueue {
	return &mockQueue{MockConn: conn, id: qtype, mf: f}
}

type mockQueue struct {
	*MockConn
	id uint16
	mf Fabric
}

func (q *mockQueue) Ack(db DB, m Model) error                             { return q.FAck(db, m) }
func (q *mockQueue) Pub(db DB, m Model, t time.Time) error                { return q.FPub(db, m, t) }
func (q *mockQueue) Sub(ctx context.Context) (<-chan Model, <-chan error) { return q.FSub(ctx) }
func (q *mockQueue) SubOne(ctx context.Context) (Model, error)            { return q.FSubOne(ctx) }
func (q *mockQueue) SubList(ctx context.Context, limit int) ([]Model, error) {
	return q.FSubList(ctx, limit)
}
func (q *mockQueue) GetLost(limit int) ([]Model, error) { return q.FGetLost(limit) }
func (q *mockQueue) Settings() (uint16, Fabric)         { return q.id, q.mf }
