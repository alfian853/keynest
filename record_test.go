package keynest

import (
	"bytes"
	"fmt"
	"testing"
)

func TestName(t *testing.T) {
	buf := &bytes.Buffer{}
	record := &Record{
		Key: "alfian",
		Val: "felicia",
	}

	m, _ := record.Marshal(buf)
	fmt.Println(m)

	record2 := &Record{}
	record2.UnMarshal(buf.Bytes())
	fmt.Println(record2.Metadata)
}
