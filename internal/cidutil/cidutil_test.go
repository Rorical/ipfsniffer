package cidutil

import "testing"

func TestExtractCIDStrings(t *testing.T) {
	valid := "bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq"
	// second "CID" is invalid and should be dropped by decoding
	s := "hello " + valid + " something " + valid + " and " + valid + "more"
	got := ExtractCIDStrings(s)
	if len(got) != 1 {
		t.Fatalf("expected 1 unique match, got %v", got)
	}
	if got[0] != valid {
		t.Fatalf("unexpected cid %q", got[0])
	}

}
