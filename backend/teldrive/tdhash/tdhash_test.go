package tdhash

import (
	"bytes"
	"testing"
)

func TestBlockHashesComposeTreeHash(t *testing.T) {
	data := bytes.Repeat([]byte("teldrive"), BlockSize/8+1)
	onePass := New()
	_, _ = onePass.Write(data)
	want := onePass.Sum(nil)

	var blocks []byte
	for offset := 0; offset < len(data); {
		end := min(offset+BlockSize, len(data))
		part := New().(*digest)
		_, _ = part.Write(data[offset:end])
		blocks = append(blocks, part.BlockHashes()...)
		offset = end
	}
	if got := SumBlockHashes(blocks); !bytes.Equal(got, want) {
		t.Fatalf("composed tree hash = %x, want %x", got, want)
	}
}
