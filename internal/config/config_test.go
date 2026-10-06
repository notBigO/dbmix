package config

import (
	"strings"
	"testing"
)

const valid = `version: 1
source: {type: postgres, uri: '${SOURCE_URI}'}
destination: {type: mysql, uri: '${DEST_URI}'}
source_quiescent: true
entities:
  - {source: public.users, target: users, key: id}
`

func TestParse(t *testing.T) {
	tests := []struct {
		name, text string
		bad        bool
	}{
		{"valid", valid, false}, {"unknown", valid + "typo: true\n", true}, {"multiple", valid + "---\nversion: 1", true}, {"mode", valid + "mode: insert\n", true}, {"workers", valid + "workers: 0\n", true}, {"secrets", strings.ReplaceAll(valid, "${SOURCE_URI}", "postgres://secret"), true}, {"quiescent", strings.ReplaceAll(valid, "source_quiescent: true", "source_quiescent: false"), true}, {"injection", strings.ReplaceAll(valid, "public.users", "users;DROP"), true}, {"duplicate", valid + "  - {source: other, target: users, key: id}\n", true}, {"unknown type", strings.ReplaceAll(valid, "postgres", "redis"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.text))
			if (err != nil) != tt.bad {
				t.Fatalf("err=%v", err)
			}
		})
	}
}
func TestResolve(t *testing.T) {
	t.Setenv("TEST_DB_URI", "password:do-not-print")
	e := Endpoint{URI: "${TEST_DB_URI}"}
	v, err := e.Resolve()
	if err != nil || v != "password:do-not-print" {
		t.Fatal(err)
	}
	e.URI = "${NONEXISTENT_DBMIX_TEST}"
	if _, err = e.Resolve(); err == nil {
		t.Fatal("missing environment accepted")
	}
}
