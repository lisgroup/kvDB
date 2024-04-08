package kvDB

import "encoding/binary"

const (
	PUT uint16 = iota
	DEL
	GET
)
const entryHeaderSize = 10

type Entry struct {
	Key      []byte
	Value    []byte
	KeyLen   uint32
	ValueLen uint32
	Mark     uint16
}

func NewEntry(k, v []byte, mark uint16) *Entry {
	return &Entry{
		Key:      k,
		Value:    v,
		KeyLen:   uint32(len(k)),
		ValueLen: uint32(len(v)),
		Mark:     mark,
	}
}

func (e *Entry) Len() int64 {
	return int64(entryHeaderSize + e.KeyLen + e.ValueLen)
}

func (e *Entry) Encode() ([]byte, error) {
	buf := make([]byte, e.Len())
	binary.BigEndian.PutUint32(buf[0:4], e.KeyLen)
	binary.BigEndian.PutUint32(buf[4:8], e.ValueLen)
	binary.BigEndian.PutUint16(buf[8:10], uint16(e.Mark))
	copy(buf[entryHeaderSize:entryHeaderSize+e.KeyLen], e.Key)
	copy(buf[entryHeaderSize+e.KeyLen:], e.Value)
	return buf, nil
}

func (e *Entry) Decode(buf []byte) {
	e.KeyLen = binary.BigEndian.Uint32(buf[0:4])
	e.ValueLen = binary.BigEndian.Uint32(buf[4:8])
	e.Mark = binary.BigEndian.Uint16(buf[8:10])
	// e.Key = buf[entryHeaderSize : entryHeaderSize+e.KeyLen]
	// e.Value = buf[entryHeaderSize+e.KeyLen:]
}
