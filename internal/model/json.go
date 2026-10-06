package model

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"unicode/utf8"
)

// JSONB and BSON cannot preserve duplicate object keys. Reject ambiguity before
// either destination silently chooses one value. Number tokens remain decimal text.
func validateJSON(raw json.RawMessage) error {
	if !utf8.Valid(raw) || !json.Valid(raw) {
		return fmt.Errorf("invalid UTF-8 JSON")
	}
	if err := jsonUnicode(raw); err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	return jsonValue(dec, 0)
}
func jsonValue(dec *json.Decoder, depth int) error {
	if depth > 100 {
		return fmt.Errorf("JSON nesting exceeds portable depth of 100")
	}
	token, err := dec.Token()
	if err != nil {
		return fmt.Errorf("invalid JSON")
	}
	delimiter, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delimiter == '{' {
		seen := map[string]bool{}
		for dec.More() {
			name, err := dec.Token()
			if err != nil {
				return fmt.Errorf("invalid JSON object")
			}
			key, ok := name.(string)
			if !ok {
				return fmt.Errorf("invalid JSON object key")
			}
			if seen[key] {
				return fmt.Errorf("duplicate JSON object key")
			}
			seen[key] = true
			if err = jsonValue(dec, depth+1); err != nil {
				return err
			}
		}
	} else {
		for dec.More() {
			if err = jsonValue(dec, depth+1); err != nil {
				return err
			}
		}
	}
	if _, err = dec.Token(); err != nil {
		return fmt.Errorf("invalid JSON closing delimiter")
	}
	return nil
}

// encoding/json replaces unpaired UTF-16 escapes with U+FFFD. Reject them rather
// than changing text while converting a SQL JSON value into a BSON document.
func jsonUnicode(raw []byte) error {
	for i := 0; i < len(raw); i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if raw[i] != 'u' {
			continue
		}
		n, _ := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		i += 4
		if n >= 0xdc00 && n <= 0xdfff {
			return fmt.Errorf("unpaired JSON Unicode surrogate")
		}
		if n >= 0xd800 && n <= 0xdbff {
			if i+6 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return fmt.Errorf("unpaired JSON Unicode surrogate")
			}
			low, _ := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if low < 0xdc00 || low > 0xdfff {
				return fmt.Errorf("unpaired JSON Unicode surrogate")
			}
			i += 6
		}
	}
	return nil
}
