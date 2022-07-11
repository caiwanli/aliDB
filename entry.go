package aliDB

import (
	"encoding/binary"
)

const (
	entryHeaderSize        = 10
	PUT             uint16 = iota
	DEL
)

//一个Entry对应一条数据  crc  k_size  v_size  key  value  makr
type Entry struct {
	KeySize   uint32
	ValueSize uint32
	Key       []byte
	Value     []byte
	Mark      uint16
}

func NewEnter(key []byte, value []byte, mark uint16) *Entry {
	return &Entry{
		KeySize:   uint32(len(key)),
		ValueSize: uint32(len(value)),
		Key:       key,
		Value:     value,
		Mark:      mark,
	}
}

func (e *Entry) GetOneEntrySize() int64 {
	//entryHeaderSize = KeySize(4byte) + ValueSize(4byte) + Mark(2byte) = 10
	return int64(entryHeaderSize + e.KeySize + e.ValueSize)
}

//将一条数据转变成字节数组
func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.GetOneEntrySize())
	binary.BigEndian.PutUint32(buf[0:4], e.KeySize)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueSize)
	binary.BigEndian.PutUint16(buf[8:10], e.Mark)
	/*buf = append(buf, e.Key...)
	buf = append(buf, e.Value...)*/
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeySize], e.Key)
	copy(buf[entryHeaderSize+e.KeySize:], e.Value)
	return buf, nil
}

//解码buf数组，还原成一条Entry
func Decode(buf []byte) (*Entry, error) {
	ks := binary.BigEndian.Uint32(buf[0:4])
	vs := binary.BigEndian.Uint32(buf[4:8])
	mark := binary.BigEndian.Uint16(buf[8:10])
	key := buf[entryHeaderSize : entryHeaderSize+ks]
	value := buf[entryHeaderSize+ks:]
	return &Entry{KeySize: ks, ValueSize: vs, Key: key, Value: value, Mark: mark}, nil
}
