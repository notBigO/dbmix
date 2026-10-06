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
