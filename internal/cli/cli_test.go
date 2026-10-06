package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestCommands(t *testing.T) {
	for _, cmd := range []string{"help", "version", "connectors"} {
		var out bytes.Buffer
		if err := Run(context.Background(), []string{cmd}, &out, &out, "test"); err != nil || out.Len() == 0 {
			t.Fatal(cmd, err)
		}
	}
}
func TestInitNoClobber(t *testing.T) {
	path := filepath.Join(t.TempDir(), "migration.yaml")
	var out bytes.Buffer
	args := []string{"init", "-f", path}
	if err := Run(context.Background(), args, &out, &out, "test"); err != nil {
		t.Fatal(err)
	}
	before, _ := os.ReadFile(path)
	if err := Run(context.Background(), args, &out, &out, "test"); err == nil {
		t.Fatal("overwrote config")
	}
	after, _ := os.ReadFile(path)
	if !bytes.Equal(before, after) {
		t.Fatal("config changed")
	}
}
