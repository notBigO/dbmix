package config

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/notBigO/dbmix/internal/model"
	"go.yaml.in/yaml/v3"
)

type Endpoint struct {
	Type          string `yaml:"type" json:"type"`
	URI           string `yaml:"uri" json:"uri"`
	Database      string `yaml:"database,omitempty" json:"database,omitempty"`
	AllowInsecure bool   `yaml:"allow_insecure,omitempty" json:"allow_insecure,omitempty"`
}
type Field struct {
	Source   string     `yaml:"source" json:"source"`
	Target   string     `yaml:"target" json:"target"`
	Type     model.Kind `yaml:"type,omitempty" json:"type,omitempty"`
	Nullable bool       `yaml:"nullable,omitempty" json:"nullable,omitempty"`
}
type Entity struct {
	Source string  `yaml:"source" json:"source"`
	Target string  `yaml:"target" json:"target"`
	Key    string  `yaml:"key" json:"key"`
	Fields []Field `yaml:"fields,omitempty" json:"fields,omitempty"`
}
type Config struct {
	Version         int      `yaml:"version" json:"version"`
	Source          Endpoint `yaml:"source" json:"source"`
	Destination     Endpoint `yaml:"destination" json:"destination"`
	Entities        []Entity `yaml:"entities" json:"entities"`
	BatchSize       int      `yaml:"batch_size" json:"batch_size"`
	Workers         int      `yaml:"workers" json:"workers"`
	QueueSize       int      `yaml:"queue_size" json:"queue_size"`
	MaxBatchBytes   int64    `yaml:"max_batch_bytes" json:"max_batch_bytes"`
	Retries         int      `yaml:"retries" json:"retries"`
	TimeoutSeconds  int      `yaml:"timeout_seconds" json:"timeout_seconds"`
	Mode            string   `yaml:"mode" json:"mode"`
	SourceQuiescent bool     `yaml:"source_quiescent" json:"source_quiescent"`
}

var identifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
var envRef = regexp.MustCompile(`^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$`)

func Identifier(s string) bool { return identifier.MatchString(s) }
func EntityName(s string) bool {
	parts := bytes.Split([]byte(s), []byte("."))
	if len(parts) > 2 {
		return false
	}
	for _, p := range parts {
		if !Identifier(string(p)) {
			return false
		}
	}
	return true
}
func (e Endpoint) Resolve() (string, error) {
	m := envRef.FindStringSubmatch(e.URI)
	if m == nil {
		return "", fmt.Errorf("connection uri must be a single environment reference, e.g. ${DB_URI}")
	}
	v, ok := os.LookupEnv(m[1])
	if !ok || v == "" {
		return "", fmt.Errorf("environment variable %s is not set", m[1])
	}
	return v, nil
}
func Load(path string) (Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	return Parse(b)
}
func Parse(b []byte) (Config, error) {
	c := Config{BatchSize: 1000, Workers: 4, QueueSize: 4, MaxBatchBytes: 16 << 20, Retries: 3, TimeoutSeconds: 30, Mode: "upsert"}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if err := d.Decode(&c); err != nil {
		return c, fmt.Errorf("invalid YAML configuration (check syntax and field names)")
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return c, fmt.Errorf("config must contain exactly one YAML document")
	}
	return c, c.Validate()
}
func (c Config) Validate() error {
	if c.Version != 1 {
		return fmt.Errorf("version must be 1")
	}
	if c.Mode != "upsert" {
		return fmt.Errorf("V1 supports only mode: upsert")
	}
	if !c.SourceQuiescent {
		return fmt.Errorf("set source_quiescent: true after stopping source writes; live migration is unsupported")
	}
	if c.BatchSize < 1 || c.BatchSize > 10000 || c.Workers < 1 || c.Workers > 32 || c.QueueSize < 1 || c.QueueSize > 64 {
		return fmt.Errorf("batch_size must be 1..10000, workers 1..32, queue_size 1..64")
	}
	if c.MaxBatchBytes < 1024 || c.MaxBatchBytes > 256<<20 || c.Retries < 0 || c.Retries > 10 || c.TimeoutSeconds < 1 || c.TimeoutSeconds > 3600 {
		return fmt.Errorf("invalid max_batch_bytes (1KiB..256MiB), retries (0..10), or timeout_seconds (1..3600)")
	}
	for _, e := range []Endpoint{c.Source, c.Destination} {
		switch e.Type {
		case "postgres", "mysql", "mongodb":
		default:
			return fmt.Errorf("unknown connector %q", e.Type)
		}
		if !envRef.MatchString(e.URI) {
			return fmt.Errorf("%s uri must reference an environment variable, e.g. ${DB_URI}", e.Type)
		}
		if e.Type == "mongodb" && !Identifier(e.Database) {
			return fmt.Errorf("mongodb requires a simple database name")
		}
	}
	if len(c.Entities) == 0 || len(c.Entities) > 100 {
		return fmt.Errorf("entities must contain 1..100 entries")
	}
	targets := map[string]bool{}
	for _, e := range c.Entities {
		if !EntityName(e.Source) || !EntityName(e.Target) || !Identifier(e.Key) {
			return fmt.Errorf("invalid entity or key identifier")
		}
		if targets[e.Target] {
			return fmt.Errorf("duplicate destination entity %s", e.Target)
		}
		targets[e.Target] = true
		if c.Source.Type == "mongodb" && len(e.Fields) == 0 {
			return fmt.Errorf("MongoDB source %s requires explicit fields and types", e.Source)
		}
		seenS, seenT := map[string]bool{}, map[string]bool{}
		for _, f := range e.Fields {
			if !Identifier(f.Source) || !Identifier(f.Target) || seenS[f.Source] || seenT[f.Target] {
				return fmt.Errorf("invalid or duplicate field mapping in %s", e.Source)
			}
			if f.Type != "" && !f.Type.Valid() {
				return fmt.Errorf("unsupported field type %q", f.Type)
			}
			if c.Source.Type == "mongodb" && !f.Type.Valid() {
				return fmt.Errorf("MongoDB fields require explicit types")
			}
			seenS[f.Source] = true
			seenT[f.Target] = true
		}
	}
	return nil
}
