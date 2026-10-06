package state

import (
	"github.com/notBigO/dbmix/internal/planner"
	"os"
	"path/filepath"
	"testing"
)

func TestDurabilityAndLock(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	m := New(planner.Plan{})
	if err = s.Save(&m); err != nil {
		t.Fatal(err)
	}
	unlock, err := s.Lock(m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Lock(m.ID); err == nil {
		t.Fatal("duplicate ownership")
	}
	s2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s2.Load(m.ID)
	if err != nil || got.Status != "planned" {
		t.Fatal(err)
	}
	if err = unlock(); err != nil {
		t.Fatal(err)
	}
	unlock, err = s2.Lock(m.ID)
	if err != nil {
		t.Fatal(err)
	}
	_ = unlock()
	st, err := os.Stat(filepath.Join(dir, "state.db"))
	if err != nil || st.Mode().Perm() != 0600 {
		t.Fatalf("permissions: %v", err)
	}
}
func TestInvalidID(t *testing.T) {
	s, _ := Open(t.TempDir())
	if _, err := s.Lock("../escape"); err == nil {
		t.Fatal("path traversal")
	}
}
