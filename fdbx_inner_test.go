package fdbx

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
)

// current test settings
var (
	TestVersion     = ConnVersion610
	TestDatabase    = uint16(0x0102)
	TestCollection  = uint16(0x0304)
	TestQueueType   = uint16(0x0506)
	TestIndexName   = uint16(0x0708)
	TestIndexNumber = uint16(0x0910)
)

var someRec = newTestRecord()

func BenchmarkFdbKey(b *testing.B) {
	rid := []byte(someRec.FdbxID())
	rln := []byte{byte(len(rid))}

	for i := 0; i < b.N; i++ {
		if len(fdbKey(TestDatabase, TestCollection, rid, rln)) != 41 {
			b.FailNow()
		}
	}
}

func BenchmarkRecKey(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if len(recKey(TestDatabase, someRec)) != 41 {
			b.FailNow()
		}
	}
}

func recordFabric(ver uint8, id string) (Record, error) { return &testRecord{ID: id}, nil }

func newTestRecord() *testRecord {
	uid := uuid.New()
	str := uid.String()
	nop := []byte{0, 0, 0, 0, 0, 0}
	num := binary.BigEndian.Uint64(append(nop, uid[:2]...))
	flt := float64(binary.BigEndian.Uint64(append(nop, uid[2:4]...)))

	return &testRecord{
		ID:      uid.String(),
		Name:    str,
		Number:  num,
		Decimal: flt,
		Logic:   flt > float64(num),
		Data:    uid[:],
		Strs:    []string{str, str, str},
	}
}

type testRecord struct {
	ID      string   `json:"-"`
	Name    string   `json:"name"`
	Number  uint64   `json:"number"`
	Decimal float64  `json:"decimal"`
	Logic   bool     `json:"logic"`
	Data    []byte   `json:"data"`
	Strs    []string `json:"strs"`

	notFound      bool
	raiseNotFound bool
}

func (r *testRecord) FdbxID() string { return r.ID }
func (r *testRecord) FdbxType() RecordType {
	return RecordType{ID: TestCollection, New: recordFabric}
}
func (r *testRecord) FdbxIndex(idx Indexer) error {
	idx.Index(TestIndexName, []byte(r.Name))
	idx.Index(TestIndexNumber, []byte(fmt.Sprintf("%d", r.Number)))
	idx.Index(TestIndexNumber, []byte(fmt.Sprintf("%d", int(r.Decimal))))
	return nil
}
func (r *testRecord) FdbxMarshal() ([]byte, error) { return json.Marshal(r) }
func (r *testRecord) FdbxUnmarshal(b []byte) error { return json.Unmarshal(b, r) }
