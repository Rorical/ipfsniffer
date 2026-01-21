package codec

import (
	"testing"

	ipfsnifferv1 "github.com/Rorical/IPFSniffer/proto"
)

func TestMarshalUnmarshal(t *testing.T) {
	in := &ipfsnifferv1.CidDiscovered{V: 1, Id: "id", Ts: "t", Data: &ipfsnifferv1.CidDiscoveredData{Cid: "bafk..."}}
	b, err := Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var out ipfsnifferv1.CidDiscovered
	if err := Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out.Id != in.Id || out.Data.Cid != in.Data.Cid {
		t.Fatalf("roundtrip mismatch: got %+v want %+v", out, in)
	}
}
