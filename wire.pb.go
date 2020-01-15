// Code generated by protoc-gen-go. DO NOT EDIT.
// source: wire.proto

package grid

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type Delivery_Ver int32

const (
	Delivery_V1 Delivery_Ver = 0
)

var Delivery_Ver_name = map[int32]string{
	0: "V1",
}

var Delivery_Ver_value = map[string]int32{
	"V1": 0,
}

func (x Delivery_Ver) String() string {
	return proto.EnumName(Delivery_Ver_name, int32(x))
}

func (Delivery_Ver) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_f2dcdddcdf68d8e0, []int{0, 0}
}

type Delivery struct {
	Ver                  Delivery_Ver `protobuf:"varint,1,opt,name=ver,proto3,enum=grid.Delivery_Ver" json:"ver,omitempty"`
	Data                 []byte       `protobuf:"bytes,2,opt,name=data,proto3" json:"data,omitempty"`
	TypeName             string       `protobuf:"bytes,3,opt,name=typeName,proto3" json:"typeName,omitempty"`
	Receiver             string       `protobuf:"bytes,4,opt,name=receiver,proto3" json:"receiver,omitempty"`
	XXX_NoUnkeyedLiteral struct{}     `json:"-"`
	XXX_unrecognized     []byte       `json:"-"`
	XXX_sizecache        int32        `json:"-"`
}

func (m *Delivery) Reset()         { *m = Delivery{} }
func (m *Delivery) String() string { return proto.CompactTextString(m) }
func (*Delivery) ProtoMessage()    {}
func (*Delivery) Descriptor() ([]byte, []int) {
	return fileDescriptor_f2dcdddcdf68d8e0, []int{0}
}

func (m *Delivery) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Delivery.Unmarshal(m, b)
}
func (m *Delivery) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Delivery.Marshal(b, m, deterministic)
}
func (m *Delivery) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Delivery.Merge(m, src)
}
func (m *Delivery) XXX_Size() int {
	return xxx_messageInfo_Delivery.Size(m)
}
func (m *Delivery) XXX_DiscardUnknown() {
	xxx_messageInfo_Delivery.DiscardUnknown(m)
}

var xxx_messageInfo_Delivery proto.InternalMessageInfo

func (m *Delivery) GetVer() Delivery_Ver {
	if m != nil {
		return m.Ver
	}
	return Delivery_V1
}

func (m *Delivery) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func (m *Delivery) GetTypeName() string {
	if m != nil {
		return m.TypeName
	}
	return ""
}

func (m *Delivery) GetReceiver() string {
	if m != nil {
		return m.Receiver
	}
	return ""
}

type ActorStart struct {
	Type                 string   `protobuf:"bytes,1,opt,name=type,proto3" json:"type,omitempty"`
	Name                 string   `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	Data                 []byte   `protobuf:"bytes,3,opt,name=data,proto3" json:"data,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *ActorStart) Reset()         { *m = ActorStart{} }
func (m *ActorStart) String() string { return proto.CompactTextString(m) }
func (*ActorStart) ProtoMessage()    {}
func (*ActorStart) Descriptor() ([]byte, []int) {
	return fileDescriptor_f2dcdddcdf68d8e0, []int{1}
}

func (m *ActorStart) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_ActorStart.Unmarshal(m, b)
}
func (m *ActorStart) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_ActorStart.Marshal(b, m, deterministic)
}
func (m *ActorStart) XXX_Merge(src proto.Message) {
	xxx_messageInfo_ActorStart.Merge(m, src)
}
func (m *ActorStart) XXX_Size() int {
	return xxx_messageInfo_ActorStart.Size(m)
}
func (m *ActorStart) XXX_DiscardUnknown() {
	xxx_messageInfo_ActorStart.DiscardUnknown(m)
}

var xxx_messageInfo_ActorStart proto.InternalMessageInfo

func (m *ActorStart) GetType() string {
	if m != nil {
		return m.Type
	}
	return ""
}

func (m *ActorStart) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *ActorStart) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

type Ack struct {
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *Ack) Reset()         { *m = Ack{} }
func (m *Ack) String() string { return proto.CompactTextString(m) }
func (*Ack) ProtoMessage()    {}
func (*Ack) Descriptor() ([]byte, []int) {
	return fileDescriptor_f2dcdddcdf68d8e0, []int{2}
}

func (m *Ack) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_Ack.Unmarshal(m, b)
}
func (m *Ack) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_Ack.Marshal(b, m, deterministic)
}
func (m *Ack) XXX_Merge(src proto.Message) {
	xxx_messageInfo_Ack.Merge(m, src)
}
func (m *Ack) XXX_Size() int {
	return xxx_messageInfo_Ack.Size(m)
}
func (m *Ack) XXX_DiscardUnknown() {
	xxx_messageInfo_Ack.DiscardUnknown(m)
}

var xxx_messageInfo_Ack proto.InternalMessageInfo

type EchoMsg struct {
	Msg                  string   `protobuf:"bytes,1,opt,name=msg,proto3" json:"msg,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *EchoMsg) Reset()         { *m = EchoMsg{} }
func (m *EchoMsg) String() string { return proto.CompactTextString(m) }
func (*EchoMsg) ProtoMessage()    {}
func (*EchoMsg) Descriptor() ([]byte, []int) {
	return fileDescriptor_f2dcdddcdf68d8e0, []int{3}
}

func (m *EchoMsg) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_EchoMsg.Unmarshal(m, b)
}
func (m *EchoMsg) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_EchoMsg.Marshal(b, m, deterministic)
}
func (m *EchoMsg) XXX_Merge(src proto.Message) {
	xxx_messageInfo_EchoMsg.Merge(m, src)
}
func (m *EchoMsg) XXX_Size() int {
	return xxx_messageInfo_EchoMsg.Size(m)
}
func (m *EchoMsg) XXX_DiscardUnknown() {
	xxx_messageInfo_EchoMsg.DiscardUnknown(m)
}

var xxx_messageInfo_EchoMsg proto.InternalMessageInfo

func (m *EchoMsg) GetMsg() string {
	if m != nil {
		return m.Msg
	}
	return ""
}

func init() {
	proto.RegisterEnum("grid.Delivery_Ver", Delivery_Ver_name, Delivery_Ver_value)
	proto.RegisterType((*Delivery)(nil), "grid.Delivery")
	proto.RegisterType((*ActorStart)(nil), "grid.ActorStart")
	proto.RegisterType((*Ack)(nil), "grid.Ack")
	proto.RegisterType((*EchoMsg)(nil), "grid.EchoMsg")
}

func init() { proto.RegisterFile("wire.proto", fileDescriptor_f2dcdddcdf68d8e0) }

var fileDescriptor_f2dcdddcdf68d8e0 = []byte{
	// 243 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x5c, 0x90, 0x4f, 0x4b, 0x03, 0x31,
	0x10, 0xc5, 0x9b, 0xcd, 0xda, 0x3f, 0x83, 0x96, 0x32, 0xa7, 0xa5, 0x5e, 0x96, 0xe0, 0x61, 0x41,
	0x58, 0xb0, 0xfd, 0x04, 0x05, 0x05, 0x2f, 0x8a, 0xac, 0xb0, 0xf7, 0x98, 0x0e, 0xeb, 0xa2, 0x35,
	0x65, 0x12, 0x2a, 0xfd, 0x0c, 0x7e, 0x69, 0x99, 0xd4, 0x56, 0xf4, 0xf6, 0xde, 0xfc, 0xc2, 0x9b,
	0x97, 0x01, 0xf8, 0xec, 0x99, 0xea, 0x2d, 0xfb, 0xe8, 0x31, 0xef, 0xb8, 0x5f, 0x9b, 0x2f, 0x05,
	0xe3, 0x5b, 0x7a, 0xef, 0x77, 0xc4, 0x7b, 0xbc, 0x02, 0xbd, 0x23, 0x2e, 0x54, 0xa9, 0xaa, 0xe9,
	0x02, 0x6b, 0x79, 0x50, 0x1f, 0x61, 0xdd, 0x12, 0x37, 0x82, 0x11, 0x21, 0x5f, 0xdb, 0x68, 0x8b,
	0xac, 0x54, 0xd5, 0x79, 0x93, 0x34, 0xce, 0x61, 0x1c, 0xf7, 0x5b, 0x7a, 0xb4, 0x1b, 0x2a, 0x74,
	0xa9, 0xaa, 0x49, 0x73, 0xf2, 0xc2, 0x98, 0x1c, 0x49, 0x4a, 0x91, 0x1f, 0xd8, 0xd1, 0x9b, 0x0b,
	0xd0, 0x2d, 0x31, 0x0e, 0x21, 0x6b, 0x6f, 0x66, 0x03, 0x73, 0x0f, 0xb0, 0x72, 0xd1, 0xf3, 0x73,
	0xb4, 0x1c, 0x65, 0x91, 0x84, 0xa4, 0x3e, 0x93, 0x26, 0x69, 0x99, 0x7d, 0xc8, 0x92, 0xec, 0x30,
	0x13, 0x7d, 0x2a, 0xa4, 0x7f, 0x0b, 0x99, 0x33, 0xd0, 0x2b, 0xf7, 0x66, 0x2e, 0x61, 0x74, 0xe7,
	0x5e, 0xfd, 0x43, 0xe8, 0x70, 0x06, 0x7a, 0x13, 0xba, 0x9f, 0x30, 0x91, 0x8b, 0x25, 0xe4, 0x72,
	0x0f, 0xbc, 0x86, 0xd1, 0x13, 0x7b, 0x47, 0x21, 0xe0, 0xf4, 0xef, 0xa7, 0xe7, 0xff, 0xbc, 0x19,
	0xbc, 0x0c, 0xd3, 0xf5, 0x96, 0xdf, 0x01, 0x00, 0x00, 0xff, 0xff, 0x1c, 0x06, 0x11, 0x27, 0x4b,
	0x01, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// WireClient is the client API for Wire service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type WireClient interface {
	Process(ctx context.Context, in *Delivery, opts ...grpc.CallOption) (*Delivery, error)
}

type wireClient struct {
	cc *grpc.ClientConn
}

func NewWireClient(cc *grpc.ClientConn) WireClient {
	return &wireClient{cc}
}

func (c *wireClient) Process(ctx context.Context, in *Delivery, opts ...grpc.CallOption) (*Delivery, error) {
	out := new(Delivery)
	err := c.cc.Invoke(ctx, "/grid.wire/Process", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// WireServer is the server API for Wire service.
type WireServer interface {
	Process(context.Context, *Delivery) (*Delivery, error)
}

// UnimplementedWireServer can be embedded to have forward compatible implementations.
type UnimplementedWireServer struct {
}

func (*UnimplementedWireServer) Process(ctx context.Context, req *Delivery) (*Delivery, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Process not implemented")
}

func RegisterWireServer(s *grpc.Server, srv WireServer) {
	s.RegisterService(&_Wire_serviceDesc, srv)
}

func _Wire_Process_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(Delivery)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(WireServer).Process(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/grid.wire/Process",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(WireServer).Process(ctx, req.(*Delivery))
	}
	return interceptor(ctx, in, info, handler)
}

var _Wire_serviceDesc = grpc.ServiceDesc{
	ServiceName: "grid.wire",
	HandlerType: (*WireServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Process",
			Handler:    _Wire_Process_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "wire.proto",
}
