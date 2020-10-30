// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package models

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type TaskHeaderT struct {
	Name string
	Text string
}

func (t *TaskHeaderT) Pack(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	if t == nil {
		return 0
	}
	nameOffset := builder.CreateString(t.Name)
	textOffset := builder.CreateString(t.Text)
	TaskHeaderStart(builder)
	TaskHeaderAddName(builder, nameOffset)
	TaskHeaderAddText(builder, textOffset)
	return TaskHeaderEnd(builder)
}

func (rcv *TaskHeader) UnPackTo(t *TaskHeaderT) {
	t.Name = string(rcv.Name())
	t.Text = string(rcv.Text())
}

func (rcv *TaskHeader) UnPack() *TaskHeaderT {
	if rcv == nil {
		return nil
	}
	t := &TaskHeaderT{}
	rcv.UnPackTo(t)
	return t
}

type TaskHeader struct {
	_tab flatbuffers.Table
}

func GetRootAsTaskHeader(buf []byte, offset flatbuffers.UOffsetT) *TaskHeader {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &TaskHeader{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *TaskHeader) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *TaskHeader) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *TaskHeader) Name() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func (rcv *TaskHeader) Text() []byte {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.ByteVector(o + rcv._tab.Pos)
	}
	return nil
}

func TaskHeaderStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func TaskHeaderAddName(builder *flatbuffers.Builder, name flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(0, flatbuffers.UOffsetT(name), 0)
}
func TaskHeaderAddText(builder *flatbuffers.Builder, text flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(text), 0)
}
func TaskHeaderEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
