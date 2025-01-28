package keynest

import (
	"bytes"
	"encoding/binary"
)

type Metadata struct {
	KeySize uint32
	ValSize uint32
	Offset  uint32
}

type Record struct {
	metadata Metadata
	Key      string
	Val      string
}

func (h *Record) Size() uint32 {
	return uint32(binary.Size(h.Key)+binary.Size(h.Val)) + h.metadata.Size()
}
func (h *Metadata) Size() uint32 {
	return uint32(binary.Size(h))
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

	err = binary.Write(src, binary.LittleEndian, &h.Offset)
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

func (r *Record) Marshal(buf *bytes.Buffer) error {
	err := r.metadata.Marshal(buf)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(r.Key)
	if err != nil {
		return err
	}
	_, err = buf.WriteString(r.Val)
	if err != nil {
		return err
	}

	return nil
}

func (r *Record) UnMarshal(src []byte) error {
	err := r.metadata.UnMarshal(src)
	if err != nil {
		return err
	}
	start := r.metadata.Size()
	end := start + r.metadata.KeySize
	r.Key = string(src[start:end])
	start, end = end, end+r.metadata.ValSize
	r.Val = string(src[start:end])

	return nil
}
