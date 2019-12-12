package fdbx

import (
	"context"
	"time"
)

func newMockQueue(conn *MockConn, rtp RecordType, prefix []byte) (*mockQueue, error) {
	return &mockQueue{MockConn: conn, rtp: rtp, pf: prefix}, nil
}

type mockQueue struct {
	*MockConn
	pf  []byte
	rtp RecordType
}

func (q *mockQueue) Ack(db DB, id ...[]byte) error                         { return q.FAck(db, id...) }
func (q *mockQueue) Pub(db DB, t time.Time, id ...[]byte) error            { return q.FPub(db, t, id...) }
func (q *mockQueue) Sub(ctx context.Context) (<-chan Record, <-chan error) { return q.FSub(ctx) }
func (q *mockQueue) SubOne(ctx context.Context) (Record, error)            { return q.FSubOne(ctx) }
func (q *mockQueue) GetLost(limit uint, filter Predicat) ([]Record, error) {
	return q.FGetLost(limit, filter)
}
func (q *mockQueue) SubList(ctx context.Context, limit uint) ([]Record, error) {
	return q.FSubList(ctx, limit)
}
