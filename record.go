package keynest

import (
	"bytes"
	"encoding/binary"
	"github.com/vmihailenco/msgpack/v5"
)

type Metadata struct {
	TombStone bool
	KeySize   uint16
	ValSize   uint32
}

type Record struct {
	Metadata
	Key string
	Val any
}

type Index struct {
	Tombstone bool
	Key       string
	KeySize   uint16
	ValSize   uint32
	Offset    int64
}

func (r *Record) ContentSize() int {
	return int(r.KeySize) + int(r.ValSize)
}

func (m *Metadata) Marshal(src *bytes.Buffer) error {
	err := binary.Write(src, binary.LittleEndian, &m.TombStone)
	if err != nil {
		return err
	}
	err = binary.Write(src, binary.LittleEndian, &m.KeySize)
	if err != nil {
		return err
	}
	err = binary.Write(src, binary.LittleEndian, &m.ValSize)
	if err != nil {
		return err
	}

	return nil
}

func (m *Metadata) UnMarshal(src []byte) error {
	start := 0
	end := 1
	_, err := binary.Decode(src[start:end], binary.LittleEndian, &m.TombStone)
	if err != nil {
		return err
	}
	start = 1
	end = 1 + binary.Size(m.KeySize)
	_, err = binary.Decode(src[start:end], binary.LittleEndian, &m.KeySize)
	if err != nil {
		return err
	}
	start, end = end, end+binary.Size(m.ValSize)
	_, err = binary.Decode(src[start:end], binary.LittleEndian, &m.ValSize)
	if err != nil {
		return err
	}
	return nil
}

func (r *Record) Marshal(buf *bytes.Buffer) (Metadata, error) {
	r.Metadata.KeySize = uint16(len(r.Key))
	b, err := msgpack.Marshal(r.Val)
	if err != nil {
		return r.Metadata, err
	}
	r.Metadata.ValSize = uint32(len(b))
	r.Metadata.Marshal(buf)

	_, err = buf.WriteString(r.Key)
	if err != nil {
		return r.Metadata, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return r.Metadata, err
	}

	return r.Metadata, nil
}

func (r *Record) UnMarshalKey(src []byte) {
	r.Key = string(src)
}

func (r *Record) UnMarshalVal(src []byte) error {
	return msgpack.Unmarshal(src, &r.Val)
}
