// Code generated by capnpc-go. DO NOT EDIT.

package schema

import (
	strconv "strconv"
	capnp "zombiezen.com/go/capnproto2"
	text "zombiezen.com/go/capnproto2/encoding/text"
	schemas "zombiezen.com/go/capnproto2/schemas"
)

type HandshakeRequest struct{ capnp.Struct }

// HandshakeRequest_TypeID is the unique identifier for the type HandshakeRequest.
const HandshakeRequest_TypeID = 0xe0d4e6d68fa24ac0

func NewHandshakeRequest(s *capnp.Segment) (HandshakeRequest, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1})
	return HandshakeRequest{st}, err
}

func NewRootHandshakeRequest(s *capnp.Segment) (HandshakeRequest, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1})
	return HandshakeRequest{st}, err
}

func ReadRootHandshakeRequest(msg *capnp.Message) (HandshakeRequest, error) {
	root, err := msg.RootPtr()
	return HandshakeRequest{root.Struct()}, err
}

func (s HandshakeRequest) String() string {
	str, _ := text.Marshal(0xe0d4e6d68fa24ac0, s.Struct)
	return str
}

func (s HandshakeRequest) Version() uint32 {
	return s.Struct.Uint32(0)
}

func (s HandshakeRequest) SetVersion(v uint32) {
	s.Struct.SetUint32(0, v)
}

func (s HandshakeRequest) VdiskID() (string, error) {
	p, err := s.Struct.Ptr(0)
	return p.Text(), err
}

func (s HandshakeRequest) HasVdiskID() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s HandshakeRequest) VdiskIDBytes() ([]byte, error) {
	p, err := s.Struct.Ptr(0)
	return p.TextBytes(), err
}

func (s HandshakeRequest) SetVdiskID(v string) error {
	return s.Struct.SetText(0, v)
}

// HandshakeRequest_List is a list of HandshakeRequest.
type HandshakeRequest_List struct{ capnp.List }

// NewHandshakeRequest creates a new list of HandshakeRequest.
func NewHandshakeRequest_List(s *capnp.Segment, sz int32) (HandshakeRequest_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1}, sz)
	return HandshakeRequest_List{l}, err
}

func (s HandshakeRequest_List) At(i int) HandshakeRequest { return HandshakeRequest{s.List.Struct(i)} }

func (s HandshakeRequest_List) Set(i int, v HandshakeRequest) error {
	return s.List.SetStruct(i, v.Struct)
}

func (s HandshakeRequest_List) String() string {
	str, _ := text.MarshalList(0xe0d4e6d68fa24ac0, s.List)
	return str
}

// HandshakeRequest_Promise is a wrapper for a HandshakeRequest promised by a client call.
type HandshakeRequest_Promise struct{ *capnp.Pipeline }

func (p HandshakeRequest_Promise) Struct() (HandshakeRequest, error) {
	s, err := p.Pipeline.Struct()
	return HandshakeRequest{s}, err
}

type HandshakeResponse struct{ capnp.Struct }

// HandshakeResponse_TypeID is the unique identifier for the type HandshakeResponse.
const HandshakeResponse_TypeID = 0xee959a7d96c96641

func NewHandshakeResponse(s *capnp.Segment) (HandshakeResponse, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 0})
	return HandshakeResponse{st}, err
}

func NewRootHandshakeResponse(s *capnp.Segment) (HandshakeResponse, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 0})
	return HandshakeResponse{st}, err
}

func ReadRootHandshakeResponse(msg *capnp.Message) (HandshakeResponse, error) {
	root, err := msg.RootPtr()
	return HandshakeResponse{root.Struct()}, err
}

func (s HandshakeResponse) String() string {
	str, _ := text.Marshal(0xee959a7d96c96641, s.Struct)
	return str
}

func (s HandshakeResponse) Version() uint32 {
	return s.Struct.Uint32(0)
}

func (s HandshakeResponse) SetVersion(v uint32) {
	s.Struct.SetUint32(0, v)
}

func (s HandshakeResponse) Status() int8 {
	return int8(s.Struct.Uint8(4))
}

func (s HandshakeResponse) SetStatus(v int8) {
	s.Struct.SetUint8(4, uint8(v))
}

func (s HandshakeResponse) LastFlushedSequence() uint64 {
	return s.Struct.Uint64(8)
}

func (s HandshakeResponse) SetLastFlushedSequence(v uint64) {
	s.Struct.SetUint64(8, v)
}

func (s HandshakeResponse) WaitTlogReady() bool {
	return s.Struct.Bit(40)
}

func (s HandshakeResponse) SetWaitTlogReady(v bool) {
	s.Struct.SetBit(40, v)
}

// HandshakeResponse_List is a list of HandshakeResponse.
type HandshakeResponse_List struct{ capnp.List }

// NewHandshakeResponse creates a new list of HandshakeResponse.
func NewHandshakeResponse_List(s *capnp.Segment, sz int32) (HandshakeResponse_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 16, PointerCount: 0}, sz)
	return HandshakeResponse_List{l}, err
}

func (s HandshakeResponse_List) At(i int) HandshakeResponse {
	return HandshakeResponse{s.List.Struct(i)}
}

func (s HandshakeResponse_List) Set(i int, v HandshakeResponse) error {
	return s.List.SetStruct(i, v.Struct)
}

func (s HandshakeResponse_List) String() string {
	str, _ := text.MarshalList(0xee959a7d96c96641, s.List)
	return str
}

// HandshakeResponse_Promise is a wrapper for a HandshakeResponse promised by a client call.
type HandshakeResponse_Promise struct{ *capnp.Pipeline }

func (p HandshakeResponse_Promise) Struct() (HandshakeResponse, error) {
	s, err := p.Pipeline.Struct()
	return HandshakeResponse{s}, err
}

type TlogAggregation struct{ capnp.Struct }

// TlogAggregation_TypeID is the unique identifier for the type TlogAggregation.
const TlogAggregation_TypeID = 0xe46ab5b4b619e094

func NewTlogAggregation(s *capnp.Segment) (TlogAggregation, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 3})
	return TlogAggregation{st}, err
}

func NewRootTlogAggregation(s *capnp.Segment) (TlogAggregation, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 3})
	return TlogAggregation{st}, err
}

func ReadRootTlogAggregation(msg *capnp.Message) (TlogAggregation, error) {
	root, err := msg.RootPtr()
	return TlogAggregation{root.Struct()}, err
}

func (s TlogAggregation) String() string {
	str, _ := text.Marshal(0xe46ab5b4b619e094, s.Struct)
	return str
}

func (s TlogAggregation) Name() (string, error) {
	p, err := s.Struct.Ptr(0)
	return p.Text(), err
}

func (s TlogAggregation) HasName() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) NameBytes() ([]byte, error) {
	p, err := s.Struct.Ptr(0)
	return p.TextBytes(), err
}

func (s TlogAggregation) SetName(v string) error {
	return s.Struct.SetText(0, v)
}

func (s TlogAggregation) Size() uint64 {
	return s.Struct.Uint64(0)
}

func (s TlogAggregation) SetSize(v uint64) {
	s.Struct.SetUint64(0, v)
}

func (s TlogAggregation) Timestamp() int64 {
	return int64(s.Struct.Uint64(8))
}

func (s TlogAggregation) SetTimestamp(v int64) {
	s.Struct.SetUint64(8, uint64(v))
}

func (s TlogAggregation) Blocks() (TlogBlock_List, error) {
	p, err := s.Struct.Ptr(1)
	return TlogBlock_List{List: p.List()}, err
}

func (s TlogAggregation) HasBlocks() bool {
	p, err := s.Struct.Ptr(1)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) SetBlocks(v TlogBlock_List) error {
	return s.Struct.SetPtr(1, v.List.ToPtr())
}

// NewBlocks sets the blocks field to a newly
// allocated TlogBlock_List, preferring placement in s's segment.
func (s TlogAggregation) NewBlocks(n int32) (TlogBlock_List, error) {
	l, err := NewTlogBlock_List(s.Struct.Segment(), n)
	if err != nil {
		return TlogBlock_List{}, err
	}
	err = s.Struct.SetPtr(1, l.List.ToPtr())
	return l, err
}

func (s TlogAggregation) Prev() ([]byte, error) {
	p, err := s.Struct.Ptr(2)
	return []byte(p.Data()), err
}

func (s TlogAggregation) HasPrev() bool {
	p, err := s.Struct.Ptr(2)
	return p.IsValid() || err != nil
}

func (s TlogAggregation) SetPrev(v []byte) error {
	return s.Struct.SetData(2, v)
}

// TlogAggregation_List is a list of TlogAggregation.
type TlogAggregation_List struct{ capnp.List }

// NewTlogAggregation creates a new list of TlogAggregation.
func NewTlogAggregation_List(s *capnp.Segment, sz int32) (TlogAggregation_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 16, PointerCount: 3}, sz)
	return TlogAggregation_List{l}, err
}

func (s TlogAggregation_List) At(i int) TlogAggregation { return TlogAggregation{s.List.Struct(i)} }

func (s TlogAggregation_List) Set(i int, v TlogAggregation) error {
	return s.List.SetStruct(i, v.Struct)
}

func (s TlogAggregation_List) String() string {
	str, _ := text.MarshalList(0xe46ab5b4b619e094, s.List)
	return str
}

// TlogAggregation_Promise is a wrapper for a TlogAggregation promised by a client call.
type TlogAggregation_Promise struct{ *capnp.Pipeline }

func (p TlogAggregation_Promise) Struct() (TlogAggregation, error) {
	s, err := p.Pipeline.Struct()
	return TlogAggregation{s}, err
}

type TlogClientMessage struct{ capnp.Struct }
type TlogClientMessage_Which uint16

const (
	TlogClientMessage_Which_block            TlogClientMessage_Which = 0
	TlogClientMessage_Which_forceFlushAtSeq  TlogClientMessage_Which = 1
	TlogClientMessage_Which_waitNBDSlaveSync TlogClientMessage_Which = 2
	TlogClientMessage_Which_disconnect       TlogClientMessage_Which = 3
)

func (w TlogClientMessage_Which) String() string {
	const s = "blockforceFlushAtSeqwaitNBDSlaveSyncdisconnect"
	switch w {
	case TlogClientMessage_Which_block:
		return s[0:5]
	case TlogClientMessage_Which_forceFlushAtSeq:
		return s[5:20]
	case TlogClientMessage_Which_waitNBDSlaveSync:
		return s[20:36]
	case TlogClientMessage_Which_disconnect:
		return s[36:46]

	}
	return "TlogClientMessage_Which(" + strconv.FormatUint(uint64(w), 10) + ")"
}

// TlogClientMessage_TypeID is the unique identifier for the type TlogClientMessage.
const TlogClientMessage_TypeID = 0xc8407b23fdf6d1a2

func NewTlogClientMessage(s *capnp.Segment) (TlogClientMessage, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 1})
	return TlogClientMessage{st}, err
}

func NewRootTlogClientMessage(s *capnp.Segment) (TlogClientMessage, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 16, PointerCount: 1})
	return TlogClientMessage{st}, err
}

func ReadRootTlogClientMessage(msg *capnp.Message) (TlogClientMessage, error) {
	root, err := msg.RootPtr()
	return TlogClientMessage{root.Struct()}, err
}

func (s TlogClientMessage) String() string {
	str, _ := text.Marshal(0xc8407b23fdf6d1a2, s.Struct)
	return str
}

func (s TlogClientMessage) Which() TlogClientMessage_Which {
	return TlogClientMessage_Which(s.Struct.Uint16(0))
}
func (s TlogClientMessage) Block() (TlogBlock, error) {
	p, err := s.Struct.Ptr(0)
	return TlogBlock{Struct: p.Struct()}, err
}

func (s TlogClientMessage) HasBlock() bool {
	if s.Struct.Uint16(0) != 0 {
		return false
	}
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s TlogClientMessage) SetBlock(v TlogBlock) error {
	s.Struct.SetUint16(0, 0)
	return s.Struct.SetPtr(0, v.Struct.ToPtr())
}

// NewBlock sets the block field to a newly
// allocated TlogBlock struct, preferring placement in s's segment.
func (s TlogClientMessage) NewBlock() (TlogBlock, error) {
	s.Struct.SetUint16(0, 0)
	ss, err := NewTlogBlock(s.Struct.Segment())
	if err != nil {
		return TlogBlock{}, err
	}
	err = s.Struct.SetPtr(0, ss.Struct.ToPtr())
	return ss, err
}

func (s TlogClientMessage) ForceFlushAtSeq() uint64 {
	return s.Struct.Uint64(8)
}

func (s TlogClientMessage) SetForceFlushAtSeq(v uint64) {
	s.Struct.SetUint16(0, 1)
	s.Struct.SetUint64(8, v)
}

func (s TlogClientMessage) SetWaitNBDSlaveSync() {
	s.Struct.SetUint16(0, 2)

}

func (s TlogClientMessage) SetDisconnect() {
	s.Struct.SetUint16(0, 3)

}

// TlogClientMessage_List is a list of TlogClientMessage.
type TlogClientMessage_List struct{ capnp.List }

// NewTlogClientMessage creates a new list of TlogClientMessage.
func NewTlogClientMessage_List(s *capnp.Segment, sz int32) (TlogClientMessage_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 16, PointerCount: 1}, sz)
	return TlogClientMessage_List{l}, err
}

func (s TlogClientMessage_List) At(i int) TlogClientMessage {
	return TlogClientMessage{s.List.Struct(i)}
}

func (s TlogClientMessage_List) Set(i int, v TlogClientMessage) error {
	return s.List.SetStruct(i, v.Struct)
}

func (s TlogClientMessage_List) String() string {
	str, _ := text.MarshalList(0xc8407b23fdf6d1a2, s.List)
	return str
}

// TlogClientMessage_Promise is a wrapper for a TlogClientMessage promised by a client call.
type TlogClientMessage_Promise struct{ *capnp.Pipeline }

func (p TlogClientMessage_Promise) Struct() (TlogClientMessage, error) {
	s, err := p.Pipeline.Struct()
	return TlogClientMessage{s}, err
}

func (p TlogClientMessage_Promise) Block() TlogBlock_Promise {
	return TlogBlock_Promise{Pipeline: p.Pipeline.GetPipeline(0)}
}

type TlogBlock struct{ capnp.Struct }

// TlogBlock_TypeID is the unique identifier for the type TlogBlock.
const TlogBlock_TypeID = 0x8cf178de3c82d431

func NewTlogBlock(s *capnp.Segment) (TlogBlock, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 32, PointerCount: 2})
	return TlogBlock{st}, err
}

func NewRootTlogBlock(s *capnp.Segment) (TlogBlock, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 32, PointerCount: 2})
	return TlogBlock{st}, err
}

func ReadRootTlogBlock(msg *capnp.Message) (TlogBlock, error) {
	root, err := msg.RootPtr()
	return TlogBlock{root.Struct()}, err
}

func (s TlogBlock) String() string {
	str, _ := text.Marshal(0x8cf178de3c82d431, s.Struct)
	return str
}

func (s TlogBlock) Sequence() uint64 {
	return s.Struct.Uint64(0)
}

func (s TlogBlock) SetSequence(v uint64) {
	s.Struct.SetUint64(0, v)
}

func (s TlogBlock) Index() int64 {
	return int64(s.Struct.Uint64(8))
}

func (s TlogBlock) SetIndex(v int64) {
	s.Struct.SetUint64(8, uint64(v))
}

func (s TlogBlock) Hash() ([]byte, error) {
	p, err := s.Struct.Ptr(0)
	return []byte(p.Data()), err
}

func (s TlogBlock) HasHash() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s TlogBlock) SetHash(v []byte) error {
	return s.Struct.SetData(0, v)
}

func (s TlogBlock) Data() ([]byte, error) {
	p, err := s.Struct.Ptr(1)
	return []byte(p.Data()), err
}

func (s TlogBlock) HasData() bool {
	p, err := s.Struct.Ptr(1)
	return p.IsValid() || err != nil
}

func (s TlogBlock) SetData(v []byte) error {
	return s.Struct.SetData(1, v)
}

func (s TlogBlock) Timestamp() int64 {
	return int64(s.Struct.Uint64(16))
}

func (s TlogBlock) SetTimestamp(v int64) {
	s.Struct.SetUint64(16, uint64(v))
}

func (s TlogBlock) Operation() uint8 {
	return s.Struct.Uint8(24)
}

func (s TlogBlock) SetOperation(v uint8) {
	s.Struct.SetUint8(24, v)
}

// TlogBlock_List is a list of TlogBlock.
type TlogBlock_List struct{ capnp.List }

// NewTlogBlock creates a new list of TlogBlock.
func NewTlogBlock_List(s *capnp.Segment, sz int32) (TlogBlock_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 32, PointerCount: 2}, sz)
	return TlogBlock_List{l}, err
}

func (s TlogBlock_List) At(i int) TlogBlock { return TlogBlock{s.List.Struct(i)} }

func (s TlogBlock_List) Set(i int, v TlogBlock) error { return s.List.SetStruct(i, v.Struct) }

func (s TlogBlock_List) String() string {
	str, _ := text.MarshalList(0x8cf178de3c82d431, s.List)
	return str
}

// TlogBlock_Promise is a wrapper for a TlogBlock promised by a client call.
type TlogBlock_Promise struct{ *capnp.Pipeline }

func (p TlogBlock_Promise) Struct() (TlogBlock, error) {
	s, err := p.Pipeline.Struct()
	return TlogBlock{s}, err
}

type TlogResponse struct{ capnp.Struct }

// TlogResponse_TypeID is the unique identifier for the type TlogResponse.
const TlogResponse_TypeID = 0x98d11ae1c78a24d9

func NewTlogResponse(s *capnp.Segment) (TlogResponse, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1})
	return TlogResponse{st}, err
}

func NewRootTlogResponse(s *capnp.Segment) (TlogResponse, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1})
	return TlogResponse{st}, err
}

func ReadRootTlogResponse(msg *capnp.Message) (TlogResponse, error) {
	root, err := msg.RootPtr()
	return TlogResponse{root.Struct()}, err
}

func (s TlogResponse) String() string {
	str, _ := text.Marshal(0x98d11ae1c78a24d9, s.Struct)
	return str
}

func (s TlogResponse) Status() int8 {
	return int8(s.Struct.Uint8(0))
}

func (s TlogResponse) SetStatus(v int8) {
	s.Struct.SetUint8(0, uint8(v))
}

func (s TlogResponse) Sequences() (capnp.UInt64List, error) {
	p, err := s.Struct.Ptr(0)
	return capnp.UInt64List{List: p.List()}, err
}

func (s TlogResponse) HasSequences() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s TlogResponse) SetSequences(v capnp.UInt64List) error {
	return s.Struct.SetPtr(0, v.List.ToPtr())
}

// NewSequences sets the sequences field to a newly
// allocated capnp.UInt64List, preferring placement in s's segment.
func (s TlogResponse) NewSequences(n int32) (capnp.UInt64List, error) {
	l, err := capnp.NewUInt64List(s.Struct.Segment(), n)
	if err != nil {
		return capnp.UInt64List{}, err
	}
	err = s.Struct.SetPtr(0, l.List.ToPtr())
	return l, err
}

// TlogResponse_List is a list of TlogResponse.
type TlogResponse_List struct{ capnp.List }

// NewTlogResponse creates a new list of TlogResponse.
func NewTlogResponse_List(s *capnp.Segment, sz int32) (TlogResponse_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 8, PointerCount: 1}, sz)
	return TlogResponse_List{l}, err
}

func (s TlogResponse_List) At(i int) TlogResponse { return TlogResponse{s.List.Struct(i)} }

func (s TlogResponse_List) Set(i int, v TlogResponse) error { return s.List.SetStruct(i, v.Struct) }

func (s TlogResponse_List) String() string {
	str, _ := text.MarshalList(0x98d11ae1c78a24d9, s.List)
	return str
}

// TlogResponse_Promise is a wrapper for a TlogResponse promised by a client call.
type TlogResponse_Promise struct{ *capnp.Pipeline }

func (p TlogResponse_Promise) Struct() (TlogResponse, error) {
	s, err := p.Pipeline.Struct()
	return TlogResponse{s}, err
}

type WaitTlogHandshakeRequest struct{ capnp.Struct }

// WaitTlogHandshakeRequest_TypeID is the unique identifier for the type WaitTlogHandshakeRequest.
const WaitTlogHandshakeRequest_TypeID = 0xb52fe5db64314d44

func NewWaitTlogHandshakeRequest(s *capnp.Segment) (WaitTlogHandshakeRequest, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 0, PointerCount: 1})
	return WaitTlogHandshakeRequest{st}, err
}

func NewRootWaitTlogHandshakeRequest(s *capnp.Segment) (WaitTlogHandshakeRequest, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 0, PointerCount: 1})
	return WaitTlogHandshakeRequest{st}, err
}

func ReadRootWaitTlogHandshakeRequest(msg *capnp.Message) (WaitTlogHandshakeRequest, error) {
	root, err := msg.RootPtr()
	return WaitTlogHandshakeRequest{root.Struct()}, err
}

func (s WaitTlogHandshakeRequest) String() string {
	str, _ := text.Marshal(0xb52fe5db64314d44, s.Struct)
	return str
}

func (s WaitTlogHandshakeRequest) VdiskID() (string, error) {
	p, err := s.Struct.Ptr(0)
	return p.Text(), err
}

func (s WaitTlogHandshakeRequest) HasVdiskID() bool {
	p, err := s.Struct.Ptr(0)
	return p.IsValid() || err != nil
}

func (s WaitTlogHandshakeRequest) VdiskIDBytes() ([]byte, error) {
	p, err := s.Struct.Ptr(0)
	return p.TextBytes(), err
}

func (s WaitTlogHandshakeRequest) SetVdiskID(v string) error {
	return s.Struct.SetText(0, v)
}

// WaitTlogHandshakeRequest_List is a list of WaitTlogHandshakeRequest.
type WaitTlogHandshakeRequest_List struct{ capnp.List }

// NewWaitTlogHandshakeRequest creates a new list of WaitTlogHandshakeRequest.
func NewWaitTlogHandshakeRequest_List(s *capnp.Segment, sz int32) (WaitTlogHandshakeRequest_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 0, PointerCount: 1}, sz)
	return WaitTlogHandshakeRequest_List{l}, err
}

func (s WaitTlogHandshakeRequest_List) At(i int) WaitTlogHandshakeRequest {
	return WaitTlogHandshakeRequest{s.List.Struct(i)}
}

func (s WaitTlogHandshakeRequest_List) Set(i int, v WaitTlogHandshakeRequest) error {
	return s.List.SetStruct(i, v.Struct)
}

func (s WaitTlogHandshakeRequest_List) String() string {
	str, _ := text.MarshalList(0xb52fe5db64314d44, s.List)
	return str
}

// WaitTlogHandshakeRequest_Promise is a wrapper for a WaitTlogHandshakeRequest promised by a client call.
type WaitTlogHandshakeRequest_Promise struct{ *capnp.Pipeline }

func (p WaitTlogHandshakeRequest_Promise) Struct() (WaitTlogHandshakeRequest, error) {
	s, err := p.Pipeline.Struct()
	return WaitTlogHandshakeRequest{s}, err
}

type WaitTlogHandshakeResponse struct{ capnp.Struct }

// WaitTlogHandshakeResponse_TypeID is the unique identifier for the type WaitTlogHandshakeResponse.
const WaitTlogHandshakeResponse_TypeID = 0xe431d9b4cf4a9e13

func NewWaitTlogHandshakeResponse(s *capnp.Segment) (WaitTlogHandshakeResponse, error) {
	st, err := capnp.NewStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0})
	return WaitTlogHandshakeResponse{st}, err
}

func NewRootWaitTlogHandshakeResponse(s *capnp.Segment) (WaitTlogHandshakeResponse, error) {
	st, err := capnp.NewRootStruct(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0})
	return WaitTlogHandshakeResponse{st}, err
}

func ReadRootWaitTlogHandshakeResponse(msg *capnp.Message) (WaitTlogHandshakeResponse, error) {
	root, err := msg.RootPtr()
	return WaitTlogHandshakeResponse{root.Struct()}, err
}

func (s WaitTlogHandshakeResponse) String() string {
	str, _ := text.Marshal(0xe431d9b4cf4a9e13, s.Struct)
	return str
}

func (s WaitTlogHandshakeResponse) Exists() bool {
	return s.Struct.Bit(0)
}

func (s WaitTlogHandshakeResponse) SetExists(v bool) {
	s.Struct.SetBit(0, v)
}

// WaitTlogHandshakeResponse_List is a list of WaitTlogHandshakeResponse.
type WaitTlogHandshakeResponse_List struct{ capnp.List }

// NewWaitTlogHandshakeResponse creates a new list of WaitTlogHandshakeResponse.
func NewWaitTlogHandshakeResponse_List(s *capnp.Segment, sz int32) (WaitTlogHandshakeResponse_List, error) {
	l, err := capnp.NewCompositeList(s, capnp.ObjectSize{DataSize: 8, PointerCount: 0}, sz)
	return WaitTlogHandshakeResponse_List{l}, err
}

func (s WaitTlogHandshakeResponse_List) At(i int) WaitTlogHandshakeResponse {
	return WaitTlogHandshakeResponse{s.List.Struct(i)}
}

func (s WaitTlogHandshakeResponse_List) Set(i int, v WaitTlogHandshakeResponse) error {
	return s.List.SetStruct(i, v.Struct)
}

func (s WaitTlogHandshakeResponse_List) String() string {
	str, _ := text.MarshalList(0xe431d9b4cf4a9e13, s.List)
	return str
}

// WaitTlogHandshakeResponse_Promise is a wrapper for a WaitTlogHandshakeResponse promised by a client call.
type WaitTlogHandshakeResponse_Promise struct{ *capnp.Pipeline }

func (p WaitTlogHandshakeResponse_Promise) Struct() (WaitTlogHandshakeResponse, error) {
	s, err := p.Pipeline.Struct()
	return WaitTlogHandshakeResponse{s}, err
}

const schema_f4533cbae6e08506 = "x\xda\x8cS]h\x1cU\x14>\xdf\xbd3\x9b\x14\xb2" +
	"I\x86Y\xa1)\x94E,X\x83\xb5I\xd7\xa7PH" +
	"\xb3\xc6R\x03\x95\xde]\xa1\x10\x14\x1dgow\xc7\xcc" +
	"\xcel\xf7\xce\xa6\x1bm\xd1\x96\x06JQ\x14Q\xa9E" +
	"\xc4\x84\x0a\x0a\x15\x14\x12\x11\x9f\xfa\xa6>I\xa1\x0aA" +
	"\x90\x08\xfd\xc1\x07\x85\x82\x08\x82u\xe4\xeef\x7f\x9a\xac" +
	"\xb6o3\x87\x8f\xef~?\xe7\x8c\xe5\xd9\x016n>" +
	"l\x10\x8913\x11\x8f_;\xb3\xff\xe7\xfa\xed\xd7I" +
	"\x8c\xc0\x88\x13\x8b\xeb7\xbf\xde\x9f\xff\x83L\xd6G\x94" +
	"\xf9\x0b;`ok|\x9a\xec(\x08\xf1\xda\xae\xf3\xdf" +
	"\xfc\xb2\xe3\xea\x05\x0dG\x17\x1c\x1a\xf3\x1c\xdf\x07\xbb\xcc" +
	"\xfb\x88l\x8f\x9f \xc4\xd3\x87\xc7\x0b?\xdd\xd8\xbbJ" +
	"\xd6\xc8\x16\xf0\x1a\x7f\x1b\xf6\xed\x06\xf87>I\x88\x97" +
	"\xaf\xfey\xe7\xa1W\x0e|\xab\xa9Y\x07\xfd$\xfa\x0c" +
	"\xa2L\xd2\x98\x85\xfd\xa0\xa1\xe1;\x8d[\x84\xf8\xca\xcc" +
	"\xf2\x9b?\xde\xbc\xb6\xdeS\x09\xcc\x1c\xec\x07L\x8d\xb6" +
	"L\xad\xc4\xfep\xe6\xfb\x95\xb5\xf1\xeb\x9b\xd0\x9a/S" +
	"6/\xc2^l\x80O\x9bZ\xc9;\xeb#_\xae\xac" +
	"\xbet}\x93\x12S\x8b\xcd,\x993\xb0W5:\xf3" +
	"\x85\x99\xd6\x99L\x1d\xfb\xee\xbdS\x17\xdf\xfd}\x13\xbc" +
	"\xc1\xfdCb\x16\xf6\xaf\x09\xcd}#q\x8b\xf6\xc4\xca" +
	"-\xc9\xb2\xb37\xe2~X|\xbe\xf9\xf3\x98\xebT\x82" +
	"\xca\xc43~X\xcc\xfa!w\xe7\x8e\x00b;7\x88" +
	"\x0c\x10Y\xef\xcf\x10\x89\x0b\x1c\xe2\x12\x83\x05\xa4\xa0\x87" +
	"K\xfb\x88\xc4\x07\x1c\xe2\x13\x06\xb0\x14\x18\x91\xf5\xf1(" +
	"\x91\xf8\x88C\\f\xb08R\xe0D\xd6\xa7zx\x89" +
	"C|\xce`\x19,\x05\x83\xc8\xfa,G$.s\x88" +
	"\xaf\x18,s{\x0a&\x91\xb5\xaa\x87+\x1c\xe2\x0aC" +
	"\xac\xe4\xf1\x9a\x0c\\ID\xd8F\x0c\xdb\x08i/(" +
	"\xc8:Lb0\x09C%G\x95\x90$\x86$a\xa8" +
	"\xe0DN\xeb'\x8e\xbc\xb2T\x91S&TZ\xe88" +
	"\xac\xc8\xaa\x13y!!@\x82\x18\x12\x84{D\x91\x93" +
	"*]\x09\x03%u\x1a\xfd\xed4\x1e\x99 \x12\xbb8" +
	"\xc4\x18C+\x8c=Z\xf9\xa3\x1c\xe2\x10\xc3\xa4\x8a\x9c" +
	"\xa8\xa6\xc0\x88\x81Q\x97\x11(\x0c\x12\x8ep4\xfc\x0c" +
	"v\xbdony\xff\xa8\xe3EZ\xc3!'(\xa8\x92" +
	"3's\x9aE!\xd2Z\x8c\xb6\x96d\x96H\xf4s" +
	"\x88\x14\xc3\xab\xf3\x05O\xcd=5\x8d\x01b\x18\xe8b" +
	"7z\xba{\xc2\xf7d\x10\x1d\x96J9\xbc\xd8\xb08" +
	"\xcc\x8d\x818n\xf0:\xba\xdcg9D\x89a'\xfe" +
	"\x897l\xca3D\xa2\xc0!*\x0cIv'n\xb6" +
	"^>O$*\x1c\xe2$C\x92\xff\x1d7k_\x98" +
	"%\x12u\x0eq\x96!\xfd\xa2\x1f\xbas\x18\xee\xdc:" +
	"\x01\xc3\x84\xf8XXu\xe5A\xbf\x06U\x9a\x8a\xf2\xf2" +
	"x\xbb\xea\xf8\x84\xe3EOg\xa7\xf3\xf0\x9dy\x99_" +
	"\x08\\\xea\x8b\x0b\x9er\xc3 \x90\xc4\xdd\x88\x12\xff\xe3" +
	"\xafgj]\x0df{4\xa8g\xbb9\xc4\xe3:I" +
	"YU^\x18\xa0\x9f\x18\xfa\xe9\xbf\x93\xbd\xaf\xdeT%" +
	"\x0cxs\x89\xba\x8a\x9b\xe8\x147)\xeb\x9e\x8a\x14@" +
	"\x0c\xb8goS\xc5bU\x16\xf5&\x07D\x9a4\xd5" +
	"&=5\xda\x89\xbc\xe5\xeb\xb4\x9e\x9d\xe4\x10\xe7\x18," +
	"\x86fc\x8bz]\xcfr\x88\xb7\xba\xee\xf4\x0d-\xe9" +
	"\xdc\xc6\x95\xb7\xeeti\xb4s\xe5C\x81S\x96\xad\x0c" +
	"\x86\x94\xf7\xb2l\xb7\xd5\xe3\xe0&\x1b\x9d\xb7W\xfe\xee" +
	"\xee\x07\x09C\x95\xaa\x9co_\xec}u\xd9\x95\xe4p" +
	"\xdb\xb4\x93\xed\xac\xaa\x05ccQ\xb5\x97\x178\x84\xdf" +
	"\xe5\xda[&\x12>\x87\xa8k\xd7\xbb\x9b\xaekU\"" +
	"\x11q\x88\xd7\xb6\xf6\xbe\xf9\x92}GE\x07\xfd\x9aB" +
	"I\x16\xf2z\xb3\xfa\x02W\xde\xb5\xb1\xba J\xe7\xa4" +
	"SXh\xd5\xf9o\x00\x00\x00\xff\xff\x0cs\xb9y"

func init() {
	schemas.Register(schema_f4533cbae6e08506,
		0x8cf178de3c82d431,
		0x98d11ae1c78a24d9,
		0xb52fe5db64314d44,
		0xc8407b23fdf6d1a2,
		0xe0d4e6d68fa24ac0,
		0xe431d9b4cf4a9e13,
		0xe46ab5b4b619e094,
		0xee959a7d96c96641)
}
