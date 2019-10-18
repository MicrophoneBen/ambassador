// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: envoy/config/filter/network/dubbo_proxy/v2alpha1/dubbo_proxy.proto

package envoy_config_filter_network_dubbo_proxy_v2alpha1

import (
	fmt "fmt"
	_ "github.com/envoyproxy/protoc-gen-validate/validate"
	proto "github.com/gogo/protobuf/proto"
	types "github.com/gogo/protobuf/types"
	io "io"
	math "math"
	math_bits "math/bits"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

// Dubbo Protocol types supported by Envoy.
type ProtocolType int32

const (
	ProtocolType_Dubbo ProtocolType = 0
)

var ProtocolType_name = map[int32]string{
	0: "Dubbo",
}

var ProtocolType_value = map[string]int32{
	"Dubbo": 0,
}

func (x ProtocolType) String() string {
	return proto.EnumName(ProtocolType_name, int32(x))
}

func (ProtocolType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_8ee9c82d7d1be64c, []int{0}
}

// Dubbo Serialization types supported by Envoy.
type SerializationType int32

const (
	SerializationType_Hessian2 SerializationType = 0
)

var SerializationType_name = map[int32]string{
	0: "Hessian2",
}

var SerializationType_value = map[string]int32{
	"Hessian2": 0,
}

func (x SerializationType) String() string {
	return proto.EnumName(SerializationType_name, int32(x))
}

func (SerializationType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_8ee9c82d7d1be64c, []int{1}
}

// [#comment:next free field: 6]
type DubboProxy struct {
	// The human readable prefix to use when emitting statistics.
	StatPrefix string `protobuf:"bytes,1,opt,name=stat_prefix,json=statPrefix,proto3" json:"stat_prefix,omitempty"`
	// Configure the protocol used.
	ProtocolType ProtocolType `protobuf:"varint,2,opt,name=protocol_type,json=protocolType,proto3,enum=envoy.config.filter.network.dubbo_proxy.v2alpha1.ProtocolType" json:"protocol_type,omitempty"`
	// Configure the serialization protocol used.
	SerializationType SerializationType `protobuf:"varint,3,opt,name=serialization_type,json=serializationType,proto3,enum=envoy.config.filter.network.dubbo_proxy.v2alpha1.SerializationType" json:"serialization_type,omitempty"`
	// The route table for the connection manager is static and is specified in this property.
	RouteConfig []*RouteConfiguration `protobuf:"bytes,4,rep,name=route_config,json=routeConfig,proto3" json:"route_config,omitempty"`
	// A list of individual Dubbo filters that make up the filter chain for requests made to the
	// Dubbo proxy. Order matters as the filters are processed sequentially. For backwards
	// compatibility, if no dubbo_filters are specified, a default Dubbo router filter
	// (`envoy.filters.dubbo.router`) is used.
	DubboFilters         []*DubboFilter `protobuf:"bytes,5,rep,name=dubbo_filters,json=dubboFilters,proto3" json:"dubbo_filters,omitempty"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *DubboProxy) Reset()         { *m = DubboProxy{} }
func (m *DubboProxy) String() string { return proto.CompactTextString(m) }
func (*DubboProxy) ProtoMessage()    {}
func (*DubboProxy) Descriptor() ([]byte, []int) {
	return fileDescriptor_8ee9c82d7d1be64c, []int{0}
}
func (m *DubboProxy) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *DubboProxy) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_DubboProxy.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *DubboProxy) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DubboProxy.Merge(m, src)
}
func (m *DubboProxy) XXX_Size() int {
	return m.Size()
}
func (m *DubboProxy) XXX_DiscardUnknown() {
	xxx_messageInfo_DubboProxy.DiscardUnknown(m)
}

var xxx_messageInfo_DubboProxy proto.InternalMessageInfo

func (m *DubboProxy) GetStatPrefix() string {
	if m != nil {
		return m.StatPrefix
	}
	return ""
}

func (m *DubboProxy) GetProtocolType() ProtocolType {
	if m != nil {
		return m.ProtocolType
	}
	return ProtocolType_Dubbo
}

func (m *DubboProxy) GetSerializationType() SerializationType {
	if m != nil {
		return m.SerializationType
	}
	return SerializationType_Hessian2
}

func (m *DubboProxy) GetRouteConfig() []*RouteConfiguration {
	if m != nil {
		return m.RouteConfig
	}
	return nil
}

func (m *DubboProxy) GetDubboFilters() []*DubboFilter {
	if m != nil {
		return m.DubboFilters
	}
	return nil
}

// DubboFilter configures a Dubbo filter.
// [#comment:next free field: 3]
type DubboFilter struct {
	// The name of the filter to instantiate. The name must match a supported
	// filter.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Filter specific configuration which depends on the filter being
	// instantiated. See the supported filters for further documentation.
	Config               *types.Any `protobuf:"bytes,2,opt,name=config,proto3" json:"config,omitempty"`
	XXX_NoUnkeyedLiteral struct{}   `json:"-"`
	XXX_unrecognized     []byte     `json:"-"`
	XXX_sizecache        int32      `json:"-"`
}

func (m *DubboFilter) Reset()         { *m = DubboFilter{} }
func (m *DubboFilter) String() string { return proto.CompactTextString(m) }
func (*DubboFilter) ProtoMessage()    {}
func (*DubboFilter) Descriptor() ([]byte, []int) {
	return fileDescriptor_8ee9c82d7d1be64c, []int{1}
}
func (m *DubboFilter) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *DubboFilter) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_DubboFilter.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *DubboFilter) XXX_Merge(src proto.Message) {
	xxx_messageInfo_DubboFilter.Merge(m, src)
}
func (m *DubboFilter) XXX_Size() int {
	return m.Size()
}
func (m *DubboFilter) XXX_DiscardUnknown() {
	xxx_messageInfo_DubboFilter.DiscardUnknown(m)
}

var xxx_messageInfo_DubboFilter proto.InternalMessageInfo

func (m *DubboFilter) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *DubboFilter) GetConfig() *types.Any {
	if m != nil {
		return m.Config
	}
	return nil
}

func init() {
	proto.RegisterEnum("envoy.config.filter.network.dubbo_proxy.v2alpha1.ProtocolType", ProtocolType_name, ProtocolType_value)
	proto.RegisterEnum("envoy.config.filter.network.dubbo_proxy.v2alpha1.SerializationType", SerializationType_name, SerializationType_value)
	proto.RegisterType((*DubboProxy)(nil), "envoy.config.filter.network.dubbo_proxy.v2alpha1.DubboProxy")
	proto.RegisterType((*DubboFilter)(nil), "envoy.config.filter.network.dubbo_proxy.v2alpha1.DubboFilter")
}

func init() {
	proto.RegisterFile("envoy/config/filter/network/dubbo_proxy/v2alpha1/dubbo_proxy.proto", fileDescriptor_8ee9c82d7d1be64c)
}

var fileDescriptor_8ee9c82d7d1be64c = []byte{
	// 443 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x94, 0x92, 0x3f, 0x8f, 0xd3, 0x30,
	0x18, 0x87, 0xcf, 0xfd, 0x73, 0xa2, 0x6f, 0x72, 0xd0, 0xb3, 0x90, 0xe8, 0x55, 0xa2, 0x2a, 0x37,
	0x55, 0x15, 0xb2, 0x21, 0xac, 0x70, 0x12, 0xb9, 0x13, 0x62, 0xac, 0x02, 0x13, 0x4b, 0xe4, 0x5c,
	0xdd, 0x62, 0x11, 0xec, 0xc8, 0x71, 0x4a, 0xc3, 0xc0, 0xc0, 0xc7, 0x62, 0x62, 0x64, 0xe4, 0x23,
	0xa0, 0x6e, 0x7c, 0x01, 0x66, 0x14, 0x3b, 0x55, 0xa3, 0xeb, 0x94, 0x2d, 0xf6, 0xef, 0xcd, 0xf3,
	0xf8, 0xf5, 0x6b, 0x08, 0xb9, 0xdc, 0xa8, 0x92, 0xde, 0x2a, 0xb9, 0x12, 0x6b, 0xba, 0x12, 0xa9,
	0xe1, 0x9a, 0x4a, 0x6e, 0xbe, 0x28, 0xfd, 0x89, 0x2e, 0x8b, 0x24, 0x51, 0x71, 0xa6, 0xd5, 0xb6,
	0xa4, 0x9b, 0x80, 0xa5, 0xd9, 0x47, 0xf6, 0xbc, 0xb9, 0x49, 0x32, 0xad, 0x8c, 0xc2, 0xcf, 0x2c,
	0x83, 0x38, 0x06, 0x71, 0x0c, 0x52, 0x33, 0x48, 0xb3, 0x7c, 0xcf, 0x18, 0xbf, 0x6c, 0x6d, 0xd5,
	0xaa, 0x30, 0xdc, 0xf9, 0xc6, 0x17, 0x6b, 0xa5, 0xd6, 0x29, 0xa7, 0x76, 0x95, 0x14, 0x2b, 0xca,
	0x64, 0x7d, 0x94, 0xf1, 0xa3, 0x0d, 0x4b, 0xc5, 0x92, 0x19, 0x4e, 0xf7, 0x1f, 0x2e, 0xb8, 0xfc,
	0xd7, 0x05, 0xb8, 0xa9, 0xc0, 0x8b, 0x8a, 0x8b, 0xe7, 0xe0, 0xe5, 0x86, 0x99, 0x38, 0xd3, 0x7c,
	0x25, 0xb6, 0x23, 0x34, 0x45, 0xb3, 0x41, 0x38, 0xf8, 0xf1, 0xf7, 0x67, 0xb7, 0xa7, 0x3b, 0x53,
	0x14, 0x41, 0x95, 0x2e, 0x6c, 0x88, 0x15, 0x9c, 0x59, 0xc6, 0xad, 0x4a, 0x63, 0x53, 0x66, 0x7c,
	0xd4, 0x99, 0xa2, 0xd9, 0xfd, 0xe0, 0x8a, 0xb4, 0x6d, 0x9b, 0x2c, 0x6a, 0xcc, 0xfb, 0x32, 0xe3,
	0x21, 0x54, 0xb6, 0xfe, 0x77, 0xd4, 0x19, 0xa2, 0xc8, 0xcf, 0x1a, 0x09, 0xfe, 0x06, 0x38, 0xe7,
	0x5a, 0xb0, 0x54, 0x7c, 0x65, 0x46, 0x28, 0xe9, 0xac, 0x5d, 0x6b, 0xbd, 0x6e, 0x6f, 0x7d, 0xd7,
	0x64, 0x1d, 0xa9, 0xcf, 0xf3, 0xbb, 0x31, 0x5e, 0x83, 0x6f, 0xaf, 0x3b, 0x76, 0x92, 0x51, 0x6f,
	0xda, 0x9d, 0x79, 0xc1, 0x4d, 0x7b, 0x73, 0x54, 0x51, 0xae, 0x6d, 0x7d, 0xa1, 0x2d, 0x3f, 0xf2,
	0xf4, 0x61, 0x0f, 0x27, 0x70, 0xe6, 0xfe, 0x73, 0xb0, 0x7c, 0xd4, 0xb7, 0xa6, 0x57, 0xed, 0x4d,
	0x76, 0xb4, 0x6f, 0x6c, 0x61, 0xe4, 0x2f, 0x0f, 0x8b, 0xfc, 0xf2, 0x03, 0x78, 0x8d, 0x10, 0x3f,
	0x86, 0x9e, 0x64, 0x9f, 0xf9, 0xf1, 0xc4, 0xed, 0x36, 0x7e, 0x0a, 0xa7, 0x75, 0xd3, 0xd5, 0x90,
	0xbd, 0xe0, 0x21, 0x71, 0x6f, 0x8d, 0xec, 0xdf, 0x1a, 0x79, 0x2d, 0xcb, 0xa8, 0xae, 0x99, 0x5f,
	0x80, 0xdf, 0x1c, 0x29, 0x1e, 0x40, 0xdf, 0xba, 0x86, 0x27, 0xf3, 0x27, 0x70, 0x7e, 0x74, 0xef,
	0xd8, 0x87, 0x7b, 0x6f, 0x79, 0x9e, 0x0b, 0x26, 0x83, 0xe1, 0x49, 0x18, 0xff, 0xda, 0x4d, 0xd0,
	0xef, 0xdd, 0x04, 0xfd, 0xd9, 0x4d, 0x10, 0x5c, 0x09, 0xe5, 0xda, 0x76, 0x9d, 0xb5, 0xbd, 0x81,
	0xf0, 0xc1, 0xe1, 0x75, 0xdb, 0x33, 0x2d, 0x50, 0x72, 0x6a, 0x0f, 0xfd, 0xe2, 0x7f, 0x00, 0x00,
	0x00, 0xff, 0xff, 0x4f, 0xef, 0x96, 0xec, 0xe4, 0x03, 0x00, 0x00,
}

func (m *DubboProxy) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *DubboProxy) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *DubboProxy) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.XXX_unrecognized != nil {
		i -= len(m.XXX_unrecognized)
		copy(dAtA[i:], m.XXX_unrecognized)
	}
	if len(m.DubboFilters) > 0 {
		for iNdEx := len(m.DubboFilters) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.DubboFilters[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintDubboProxy(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x2a
		}
	}
	if len(m.RouteConfig) > 0 {
		for iNdEx := len(m.RouteConfig) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.RouteConfig[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintDubboProxy(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x22
		}
	}
	if m.SerializationType != 0 {
		i = encodeVarintDubboProxy(dAtA, i, uint64(m.SerializationType))
		i--
		dAtA[i] = 0x18
	}
	if m.ProtocolType != 0 {
		i = encodeVarintDubboProxy(dAtA, i, uint64(m.ProtocolType))
		i--
		dAtA[i] = 0x10
	}
	if len(m.StatPrefix) > 0 {
		i -= len(m.StatPrefix)
		copy(dAtA[i:], m.StatPrefix)
		i = encodeVarintDubboProxy(dAtA, i, uint64(len(m.StatPrefix)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *DubboFilter) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *DubboFilter) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *DubboFilter) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.XXX_unrecognized != nil {
		i -= len(m.XXX_unrecognized)
		copy(dAtA[i:], m.XXX_unrecognized)
	}
	if m.Config != nil {
		{
			size, err := m.Config.MarshalToSizedBuffer(dAtA[:i])
			if err != nil {
				return 0, err
			}
			i -= size
			i = encodeVarintDubboProxy(dAtA, i, uint64(size))
		}
		i--
		dAtA[i] = 0x12
	}
	if len(m.Name) > 0 {
		i -= len(m.Name)
		copy(dAtA[i:], m.Name)
		i = encodeVarintDubboProxy(dAtA, i, uint64(len(m.Name)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintDubboProxy(dAtA []byte, offset int, v uint64) int {
	offset -= sovDubboProxy(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *DubboProxy) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.StatPrefix)
	if l > 0 {
		n += 1 + l + sovDubboProxy(uint64(l))
	}
	if m.ProtocolType != 0 {
		n += 1 + sovDubboProxy(uint64(m.ProtocolType))
	}
	if m.SerializationType != 0 {
		n += 1 + sovDubboProxy(uint64(m.SerializationType))
	}
	if len(m.RouteConfig) > 0 {
		for _, e := range m.RouteConfig {
			l = e.Size()
			n += 1 + l + sovDubboProxy(uint64(l))
		}
	}
	if len(m.DubboFilters) > 0 {
		for _, e := range m.DubboFilters {
			l = e.Size()
			n += 1 + l + sovDubboProxy(uint64(l))
		}
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *DubboFilter) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Name)
	if l > 0 {
		n += 1 + l + sovDubboProxy(uint64(l))
	}
	if m.Config != nil {
		l = m.Config.Size()
		n += 1 + l + sovDubboProxy(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovDubboProxy(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozDubboProxy(x uint64) (n int) {
	return sovDubboProxy(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *DubboProxy) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDubboProxy
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: DubboProxy: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: DubboProxy: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StatPrefix", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthDubboProxy
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.StatPrefix = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field ProtocolType", wireType)
			}
			m.ProtocolType = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.ProtocolType |= ProtocolType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field SerializationType", wireType)
			}
			m.SerializationType = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.SerializationType |= SerializationType(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field RouteConfig", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthDubboProxy
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.RouteConfig = append(m.RouteConfig, &RouteConfiguration{})
			if err := m.RouteConfig[len(m.RouteConfig)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field DubboFilters", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthDubboProxy
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.DubboFilters = append(m.DubboFilters, &DubboFilter{})
			if err := m.DubboFilters[len(m.DubboFilters)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipDubboProxy(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *DubboFilter) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowDubboProxy
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: DubboFilter: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: DubboFilter: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Name", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthDubboProxy
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Name = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Config", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthDubboProxy
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if m.Config == nil {
				m.Config = &types.Any{}
			}
			if err := m.Config.Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipDubboProxy(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthDubboProxy
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipDubboProxy(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowDubboProxy
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowDubboProxy
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthDubboProxy
			}
			iNdEx += length
			if iNdEx < 0 {
				return 0, ErrInvalidLengthDubboProxy
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowDubboProxy
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipDubboProxy(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
				if iNdEx < 0 {
					return 0, ErrInvalidLengthDubboProxy
				}
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthDubboProxy = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowDubboProxy   = fmt.Errorf("proto: integer overflow")
)