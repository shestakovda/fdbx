// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package models

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type TaskStateT struct {
	Status  byte
	Repeats uint32
	Created int64
	Planned int64
}

func (t *TaskStateT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	return CreateTaskState(builder, t.Status, t.Repeats, t.Created, t.Planned)
}
func (rcv *TaskState) UnPackTo(t *TaskStateT) {
	t.Status = rcv.Status()
	t.Repeats = rcv.Repeats()
	t.Created = rcv.Created()
	t.Planned = rcv.Planned()
}

func (rcv *TaskState) UnPack() *TaskStateT {
	if rcv == nil {
		return nil
	}
	t := &TaskStateT{}
	rcv.UnPackTo(t)
	return t
}

type TaskState struct {
	_tab flatbuffers.Struct
}

func (rcv *TaskState) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TaskState) Table() flatbuffers.Table {
	return rcv._tab.Table
}

func (rcv *TaskState) Status() byte {
	return rcv._tab.GetByte(rcv._tab.Pos + flatbuffers.UOffsetT(0))
}
func (rcv *TaskState) MutateStatus(n byte) bool {
	return rcv._tab.MutateByte(rcv._tab.Pos+flatbuffers.UOffsetT(0), n)
}

func (rcv *TaskState) Repeats() uint32 {
	return rcv._tab.GetUint32(rcv._tab.Pos + flatbuffers.UOffsetT(4))
}
func (rcv *TaskState) MutateRepeats(n uint32) bool {
	return rcv._tab.MutateUint32(rcv._tab.Pos+flatbuffers.UOffsetT(4), n)
}

func (rcv *TaskState) Created() int64 {
	return rcv._tab.GetInt64(rcv._tab.Pos + flatbuffers.UOffsetT(8))
}
func (rcv *TaskState) MutateCreated(n int64) bool {
	return rcv._tab.MutateInt64(rcv._tab.Pos+flatbuffers.UOffsetT(8), n)
}

func (rcv *TaskState) Planned() int64 {
	return rcv._tab.GetInt64(rcv._tab.Pos + flatbuffers.UOffsetT(16))
}
func (rcv *TaskState) MutatePlanned(n int64) bool {
	return rcv._tab.MutateInt64(rcv._tab.Pos+flatbuffers.UOffsetT(16), n)
}

func CreateTaskState(builder *flatbuffers.Builder, status byte, repeats uint32, created int64, planned int64) flatbuffers.UOffsetT {
	builder.Prep(8, 24)
	builder.PrependInt64(planned)
	builder.PrependInt64(created)
	builder.PrependUint32(repeats)
	builder.Pad(3)
	builder.PrependByte(status)
	return builder.Offset()
}
