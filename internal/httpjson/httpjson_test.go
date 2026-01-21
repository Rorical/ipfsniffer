package httpjson

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestWrite_JSON(t *testing.T) {
	rr := httptest.NewRecorder()
	Write(rr, 201, map[string]any{"ok": true})

	if rr.Code != 201 {
		t.Fatalf("status: %d", rr.Code)
	}
	if ct := rr.Header().Get("content-type"); ct == "" {
		t.Fatalf("expected content-type")
	}
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("json: %v", err)
	}
	if out["ok"] != true {
		t.Fatalf("body: %+v", out)
	}
}

func TestError_JSON(t *testing.T) {
	rr := httptest.NewRecorder()
	Error(rr, 400, "bad")
	if rr.Code != 400 {
		t.Fatalf("status: %d", rr.Code)
	}
	var out map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &out); err != nil {
		t.Fatalf("json: %v", err)
	}
	if out["error"] != "bad" {
		t.Fatalf("body: %+v", out)
	}
}
