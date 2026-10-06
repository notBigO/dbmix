package connectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func jsonToBSON(raw json.RawMessage, field string) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, fmt.Errorf("field %s: invalid JSON", field)
	}
	result, err := jsonValue(v)
	if err != nil {
		return nil, fmt.Errorf("field %s: %w", field, err)
	}
	return result, nil
}
func jsonValue(v any) (any, error) {
	switch x := v.(type) {
	case json.Number:
		if !strings.ContainsAny(string(x), ".eE") {
			i, err := x.Int64()
			if err != nil {
				return nil, fmt.Errorf("JSON integer exceeds BSON int64")
			}
			return i, nil
		}
		f, err := x.Float64()
		if err != nil {
			return nil, fmt.Errorf("JSON number exceeds BSON double")
		}
		// Reject values whose decimal spelling changes under binary64 round-trip.
		// JSON's exact decimal intent is ambiguous; this deliberately errs conservatively.
		if strconv.FormatFloat(f, 'g', -1, 64) != string(x) {
			return nil, fmt.Errorf("JSON decimal needs explicit normalization before BSON conversion")
		}
		return f, nil
	case map[string]any:
		out := bson.M{}
		for k, item := range x {
			y, err := jsonValue(item)
			if err != nil {
				return nil, err
			}
			out[k] = y
		}
		return out, nil
	case []any:
		out := make(bson.A, len(x))
		for i, item := range x {
			y, err := jsonValue(item)
			if err != nil {
				return nil, err
			}
			out[i] = y
		}
		return out, nil
	default:
		return x, nil
	}
}
