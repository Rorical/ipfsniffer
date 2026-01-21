package codec

import (
	"testing"

	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"
)

func TestMarshal_Nil(t *testing.T) {
	if _, err := Marshal(nil); err == nil {
		t.Fatalf("expected error")
	}
}

func TestUnmarshal_Nil(t *testing.T) {
	if err := Unmarshal([]byte{1, 2, 3}, nil); err == nil {
		t.Fatalf("expected error")
	}
}

func TestUnmarshal_Empty(t *testing.T) {
	var m ipfsnifferv1.CidDiscovered
	if err := Unmarshal(nil, &m); err == nil {
		t.Fatalf("expected error")
	}
}

func TestUnmarshal_BadData(t *testing.T) {
	var m ipfsnifferv1.CidDiscovered
	if err := Unmarshal([]byte{0xff, 0xfe, 0xfd}, &m); err == nil {
		t.Fatalf("expected error")
	}
}
