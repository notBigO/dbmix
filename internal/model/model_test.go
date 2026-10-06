package model

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestNormalize(t *testing.T) {
	tests := []struct {
		k             Kind
		v             any
		nullable, bad bool
	}{{Int64, int64(1), false, false}, {Int64, 1.5, false, true}, {Float64, math.Inf(1), false, true}, {String, nil, true, false}, {String, nil, false, true}, {JSON, json.RawMessage(`{"x":1}`), false, false}, {JSON, json.RawMessage(`{bad}`), false, true}, {Time, time.Now(), false, false}, {Bool, 1, false, true}}
	for _, tt := range tests {
		_, err := Normalize(tt.v, Field{Name: "x", Type: tt.k, Nullable: tt.nullable})
		if (err != nil) != tt.bad {
			t.Fatalf("%+v: %v", tt, err)
		}
	}
}
func TestTransform(t *testing.T) {
	r := Record{"id": int64(7), "drop": "private"}
	out, n, err := Transform([]Record{r}, []Projection{{Source: "id", Target: Field{Name: "_id", Type: Int64}}})
	if err != nil || n == 0 || len(out[0]) != 1 || out[0]["_id"] != int64(7) {
		t.Fatalf("%v %v", out, err)
	}
	if r["id"] != int64(7) {
		t.Fatal("mutated source")
	}
}

func TestEmptyBinaryIsNotNull(t *testing.T) {
	v, err := Normalize([]byte(nil), Field{Name: "payload", Type: Bytes})
	if err != nil || v.([]byte) == nil {
		t.Fatalf("empty binary would become SQL NULL: %v", err)
	}
}

func TestAmbiguousJSONRejected(t *testing.T) {
	for _, raw := range []string{`{"a":1,"a":2}`, `{"nested":[{"key":1,"key":2}]}`} {
		if _, err := Normalize(json.RawMessage(raw), Field{Name: "data", Type: JSON}); err == nil {
			t.Fatal("duplicate key was silently discarded")
		}
	}
	if _, err := Normalize(json.RawMessage(`{"a":[1,2],"b":{"a":1}}`), Field{Name: "data", Type: JSON}); err != nil {
		t.Fatal(err)
	}
}

func TestJSONUnicodeEscapes(t *testing.T) {
	for _, tt := range []struct {
		raw string
		bad bool
	}{{`"\ud800"`, true}, {`"\udfff"`, true}, {`"\ud800\u1234"`, true}, {`"\ud83d\ude00"`, false}, {`"\\ud800"`, false}, {`"\ufffd"`, false}} {
		if err := validateJSON(json.RawMessage(tt.raw)); (err != nil) != tt.bad {
			t.Fatalf("%s: %v", tt.raw, err)
		}
	}
}
