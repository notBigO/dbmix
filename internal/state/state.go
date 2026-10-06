// Package state keeps credentials out of durable plans and serializes migration ownership.
package state

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"time"

	"github.com/gofrs/flock"
	"github.com/notBigO/dbmix/internal/model"
	"github.com/notBigO/dbmix/internal/planner"
	bolt "go.etcd.io/bbolt"
)

type Progress struct {
	Cursor  model.Cursor `json:"cursor"`
	Records int64        `json:"records"`
	Bytes   int64        `json:"bytes"`
	Done    bool         `json:"done"`
}
type Migration struct {
	EndpointFingerprint string       `json:"endpoint_fingerprint"`
	Version             int          `json:"version"`
	ID                  string       `json:"id"`
	Status              string       `json:"status"`
	Plan                planner.Plan `json:"plan"`
	Progress            []Progress   `json:"progress"`
	CreatedAt           time.Time    `json:"created_at"`
	UpdatedAt           time.Time    `json:"updated_at"`
	ElapsedSeconds      float64      `json:"elapsed_seconds"`
	LastError           string       `json:"last_error,omitempty"`
	FailedBatches       int64        `json:"failed_batches"`
}

func New(p planner.Plan) Migration {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	now := time.Now().UTC()
	return Migration{Version: 1, ID: hex.EncodeToString(b), Status: "planned", Plan: p, Progress: make([]Progress, len(p.Entities)), CreatedAt: now, UpdatedAt: now}
}
func (m Migration) Totals() (int64, int64) {
	var records, bytes int64
	for _, p := range m.Progress {
		records += p.Records
		bytes += p.Bytes
	}
	return records, bytes
}

type Store struct{ Dir string }

var idPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	st, err := os.Lstat(dir)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() || st.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("state directory must be a real directory")
	}
	if err = os.Chmod(dir, 0700); err != nil {
		return nil, err
	}
	return &Store{Dir: dir}, nil
}
func (s *Store) transaction(write bool, fn func(*bolt.Tx) error) error {
	// Open briefly: status can run while a migration owns its separate run lock.
	db, err := bolt.Open(filepath.Join(s.Dir, "state.db"), 0600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return fmt.Errorf("open state database: %w", err)
	}
	var opErr error
	if write {
		opErr = db.Update(fn)
	} else {
		opErr = db.View(fn)
	}
	closeErr := db.Close()
	if opErr != nil {
		return opErr
	}
	return closeErr
}
func (s *Store) Save(m *Migration) error {
	if !idPattern.MatchString(m.ID) {
		return fmt.Errorf("invalid migration ID")
	}
	m.UpdatedAt = time.Now().UTC()
	b, err := json.Marshal(m)
	if err != nil {
		return err
	}
	return s.transaction(true, func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte("migrations"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte(m.ID), b)
	})
}
func (s *Store) Load(id string) (Migration, error) {
	var m Migration
	if !idPattern.MatchString(id) {
		return m, fmt.Errorf("invalid migration ID")
	}
	err := s.transaction(false, func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("migrations"))
		if b == nil || b.Get([]byte(id)) == nil {
			return fmt.Errorf("migration %s not found", id)
		}
		return json.Unmarshal(b.Get([]byte(id)), &m)
	})
	if err == nil && (m.Version != 1 || len(m.Progress) != len(m.Plan.Entities)) {
		err = fmt.Errorf("unsupported or corrupt migration state")
	}
	return m, err
}
func (s *Store) Lock(id string) (func() error, error) {
	if !idPattern.MatchString(id) {
		return nil, fmt.Errorf("invalid migration ID")
	}
	l := flock.New(filepath.Join(s.Dir, id+".lock"))
	ok, err := l.TryLock()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("migration %s is already running", id)
	}
	return l.Close, nil
}
