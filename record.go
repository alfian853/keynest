package keynest

import (
	"bytes"
	"encoding/binary"
	"github.com/vmihailenco/msgpack/v5"
)

type Metadata struct {
	KeySize uint16
	ValSize uint32
}

type Record struct {
	Key string
	Val any
}

type Index struct {
	Key     string
	KeySize uint16
	ValSize uint32
	Offset  int64
}

func (h *Metadata) Marshal(src *bytes.Buffer) error {
	err := binary.Write(src, binary.LittleEndian, &h.KeySize)
	if err != nil {
		return err
	}
	err = binary.Write(src, binary.LittleEndian, &h.ValSize)
	if err != nil {
		return err
	}

	return nil
}

func (h *Metadata) UnMarshal(src []byte) error {
	start := 0
	end := binary.Size(h.KeySize)
	_, err := binary.Decode(src[start:end], binary.LittleEndian, &h.KeySize)
	if err != nil {
		return err
	}
	start, end = end, end+binary.Size(h.ValSize)
	_, err = binary.Decode(src[start:end], binary.LittleEndian, &h.ValSize)
	if err != nil {
		return err
	}
	return nil
}

func (r *Record) Marshal(buf *bytes.Buffer) (Metadata, error) {
	metadata := Metadata{}
	metadata.KeySize = uint16(len(r.Key))
	b, err := msgpack.Marshal(r.Val)
	if err != nil {
		return metadata, err
	}
	metadata.ValSize = uint32(len(b))
	metadata.Marshal(buf)

	_, err = buf.WriteString(r.Key)
	if err != nil {
		return metadata, err
	}

	_, err = buf.Write(b)
	if err != nil {
		return metadata, err
	}

	return metadata, nil
}

func (r *Record) UnMarshalKey(src []byte) {
	r.Key = string(src)
}

func (r *Record) UnMarshalVal(src []byte) error {
	return msgpack.Unmarshal(src, &r.Val)
}
