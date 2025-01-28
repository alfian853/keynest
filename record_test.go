package keynest

import (
	"bytes"
	"testing"
)

func TestRecordMarshalUnMarshal(t *testing.T) {
	originalRecord := Record{
		Key: "testKey",
		Val: "testValue",
		metadata: Metadata{
			KeySize: 7,
			ValSize: 9,
			Offset:  5,
		},
	}

	// Marshal the record
	buf := new(bytes.Buffer)
	err := originalRecord.Marshal(buf)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}

	// UnMarshal the record
	var unmarshaledRecord Record
	err = unmarshaledRecord.UnMarshal(buf.Bytes())
	if err != nil {
		t.Fatalf("UnMarshal failed: %v", err)
	}

	// Compare the original and unmarshaled records
	if originalRecord.Key != unmarshaledRecord.Key || originalRecord.Val != unmarshaledRecord.Val {
		t.Errorf("Unmarshaled record does not match original. Got %+v, want %+v", unmarshaledRecord, originalRecord)
	}
}
