// Package model defines the small, loss-aware type vocabulary shared by connectors.
package model

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

type Kind string

const (
	Int64    Kind = "int64"
	Float64  Kind = "float64"
	String   Kind = "string"
	Bool     Kind = "bool"
	Time     Kind = "datetime"
	JSON     Kind = "json"
	Bytes    Kind = "bytes"
	ObjectID Kind = "objectid"
)

func (k Kind) Valid() bool {
	switch k {
	case Int64, Float64, String, Bool, Time, JSON, Bytes, ObjectID:
		return true
	}
	return false
}

type Field struct {
	Name     string `json:"name" yaml:"name"`
	Type     Kind   `json:"type" yaml:"type"`
	Nullable bool   `json:"nullable" yaml:"nullable"`
}
type Schema struct {
	Fields []Field `json:"fields"`
	Key    string  `json:"key"`
}

func (s Schema) Field(name string) (Field, bool) {
	for _, f := range s.Fields {
		if f.Name == name {
			return f, true
		}
	}
	return Field{}, false
}

type Record map[string]any

// Cursor is source-owned. An empty cursor means before the first record, not key zero.
type Cursor string

type Inspection struct {
	Schema        Schema `json:"schema"`
	Upper         Cursor `json:"upper"`
	EstimatedRows int64  `json:"estimated_rows"`
}
type Projection struct {
	Source string `json:"source"`
	Target Field  `json:"target"`
}

// Normalize refuses coercions that hide data loss. Connectors normalize driver-specific
// values first; transformations then enforce the planned contract for every record.
func Normalize(v any, f Field) (any, error) {
	if v == nil {
		if f.Nullable {
			return nil, nil
		}
		return nil, fmt.Errorf("field %s is null or missing", f.Name)
	}
	bad := func() (any, error) { return nil, fmt.Errorf("field %s: expected %s, got %T", f.Name, f.Type, v) }
	switch f.Type {
	case Int64:
		switch x := v.(type) {
		case int64:
			return x, nil
		case int32:
			return int64(x), nil
		case int:
			return int64(x), nil
		}
	case Float64:
		if x, ok := v.(float64); ok && !math.IsInf(x, 0) && !math.IsNaN(x) {
			return x, nil
		}
	case String, ObjectID:
		if x, ok := v.(string); ok {
			return x, nil
		}
	case Bool:
		if x, ok := v.(bool); ok {
			return x, nil
		}
	case Time:
		if x, ok := v.(time.Time); ok {
			return x.UTC(), nil
		}
	case Bytes:
		if x, ok := v.([]byte); ok {
			return x, nil
		}
	case JSON:
		if x, ok := v.(json.RawMessage); ok && json.Valid(x) {
			return x, nil
		}
	}
	return bad()
}
func Transform(in []Record, fields []Projection) ([]Record, int64, error) {
	out := make([]Record, 0, len(in))
	var size int64
	for _, r := range in {
		dst := make(Record, len(fields))
		for _, f := range fields {
			v, err := Normalize(r[f.Source], f.Target)
			if err != nil {
				return nil, 0, err
			}
			dst[f.Target.Name] = v
		}
		b, err := json.Marshal(dst)
		if err != nil {
			return nil, 0, fmt.Errorf("measure transformed record: %w", err)
		}
		size += int64(len(b))
		out = append(out, dst)
	}
	return out, size, nil
}
func Key(v any, kind Kind) (Cursor, error) {
	switch kind {
	case Int64:
		if x, ok := v.(int64); ok {
			return Cursor(strconv.FormatInt(x, 10)), nil
		}
	case ObjectID:
		if x, ok := v.(string); ok && len(x) == 24 {
			return Cursor(x), nil
		}
	}
	return "", fmt.Errorf("invalid %s cursor value", kind)
}
