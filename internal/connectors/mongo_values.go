package connectors

import (
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/notBigO/dbmix/internal/model"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func decodeMongo(v any, f model.Field) (any, error) {
	if v == nil {
		return model.Normalize(nil, f)
	}
	switch f.Type {
	case model.ObjectID:
		if x, ok := v.(bson.ObjectID); ok {
			v = x.Hex()
		}
	case model.Time:
		if x, ok := v.(bson.DateTime); ok {
			v = x.Time()
		}
	case model.Bytes:
		if x, ok := v.(bson.Binary); ok && x.Subtype == 0 {
			v = x.Data
		}
	case model.JSON:
		plain, err := plainJSON(v)
		if err != nil {
			return nil, fmt.Errorf("field %s: %w", f.Name, err)
		}
		raw, err := json.Marshal(plain)
		if err != nil {
			return nil, fmt.Errorf("field %s: invalid JSON value", f.Name)
		}
		v = json.RawMessage(raw)
	}
	return model.Normalize(v, f)
}

// Nested BSON-only types require an explicit preprocessing policy. Extended JSON
// wrappers would look like ordinary application objects after a round trip.
func plainJSON(v any) (any, error) {
	switch x := v.(type) {
	case nil, string, bool, int32, int64:
		return x, nil
	case float64:
		if !math.IsNaN(x) && !math.IsInf(x, 0) {
			return x, nil
		}
	case bson.M:
		out := map[string]any{}
		for k, item := range x {
			p, err := plainJSON(item)
			if err != nil {
				return nil, err
			}
			out[k] = p
		}
		return out, nil
	case bson.D:
		out := map[string]any{}
		for _, item := range x {
			if _, exists := out[item.Key]; exists {
				return nil, fmt.Errorf("duplicate nested document field")
			}
			p, err := plainJSON(item.Value)
			if err != nil {
				return nil, err
			}
			out[item.Key] = p
		}
		return out, nil
	case bson.A:
		out := make([]any, len(x))
		for i, item := range x {
			p, err := plainJSON(item)
			if err != nil {
				return nil, err
			}
			out[i] = p
		}
		return out, nil
	}
	return nil, fmt.Errorf("nested BSON type %T cannot be represented as plain JSON", v)
}
func encodeMongo(v any, f model.Field) (any, error) {
	if v == nil {
		return nil, nil
	}
	switch f.Type {
	case model.ObjectID:
		x, err := bson.ObjectIDFromHex(v.(string))
		if err != nil {
			return nil, fmt.Errorf("field %s: invalid ObjectID", f.Name)
		}
		return x, nil
	case model.Time:
		t := v.(time.Time)
		if t.Nanosecond()%int(time.Millisecond) != 0 {
			return nil, fmt.Errorf("field %s exceeds MongoDB millisecond precision; preprocess timestamps explicitly", f.Name)
		}
		return t, nil
	case model.JSON:
		// Decode ordinary JSON ourselves: Extended JSON decoding would interpret user
		// objects such as {"$date": ...} as typed BSON values.
		return jsonToBSON(v.(json.RawMessage), f.Name)
	case model.Bytes:
		return bson.Binary{Subtype: 0, Data: v.([]byte)}, nil
	}
	return v, nil
}
