//go:build integration

package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/connector"
	"github.com/notBigO/dbmix/internal/connectors"
	"github.com/notBigO/dbmix/internal/engine"
	"github.com/notBigO/dbmix/internal/model"
	"github.com/notBigO/dbmix/internal/planner"
	"github.com/notBigO/dbmix/internal/state"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var pg *sql.DB
var my *sql.DB
var mg *mongo.Client

const pgURI = "postgres://dbmix:dbmix@127.0.0.1:55432/dbmix?sslmode=disable"
const myURI = "dbmix:dbmix@tcp(127.0.0.1:53306)/dbmix?parseTime=true&loc=UTC"
const mgURI = "mongodb://127.0.0.1:57017"

func TestMain(m *testing.M) {
	var err error
	pg, err = sql.Open("pgx", pgURI)
	if err != nil {
		panic(err)
	}
	my, err = sql.Open("mysql", myURI)
	if err != nil {
		panic(err)
	}
	mg, err = mongo.Connect(options.Client().ApplyURI(mgURI))
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, db := range []*sql.DB{pg, my} {
		if err = db.PingContext(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "Run docker compose up -d --wait before integration tests:", err)
			os.Exit(1)
		}
	}
	if err = mg.Ping(ctx, nil); err != nil {
		panic(err)
	}
	code := m.Run()
	_ = pg.Close()
	_ = my.Close()
	_ = mg.Disconnect(context.Background())
	os.Exit(code)
}
func endpoint(t *testing.T, kind, side string) config.Endpoint {
	t.Helper()
	uri := map[string]string{"postgres": pgURI, "mysql": myURI, "mongodb": mgURI}[kind]
	name := "DBMIX_TEST_" + side
	t.Setenv(name, uri)
	e := config.Endpoint{Type: kind, URI: "${" + name + "}", AllowInsecure: true, AssumeUTC: kind == "mysql"}
	if kind == "mongodb" {
		e.Database = "dbmix"
	}
	return e
}
func key(kind string) string {
	if kind == "mongodb" {
		return "_id"
	}
	return "id"
}
func fields(src, dst string) []config.Field {
	return []config.Field{{Source: key(src), Target: key(dst), Type: model.Int64}, {Source: "name", Target: "name", Type: model.String}, {Source: "enabled", Target: "enabled", Type: model.Bool}, {Source: "score", Target: "score", Type: model.Float64}, {Source: "created_at", Target: "created_at", Type: model.Time}, {Source: "metadata", Target: "metadata", Type: model.JSON}, {Source: "payload", Target: "payload", Type: model.Bytes}, {Source: "optional", Target: "optional", Type: model.String, Nullable: true}}
}
func cfg(t *testing.T, src, dst, source, target string) config.Config {
	t.Helper()
	return config.Config{Version: 1, Source: endpoint(t, src, "SRC"), Destination: endpoint(t, dst, "DST"), SourceQuiescent: true, Mode: "upsert", BatchSize: 17, Workers: 4, QueueSize: 2, MaxBatchBytes: 1 << 20, Retries: 2, TimeoutSeconds: 10, Entities: []config.Entity{{Source: source, Target: target, Key: key(src), Fields: fields(src, dst)}}}
}
func seed(t *testing.T, kind, table string, count int) {
	t.Helper()
	ctx := context.Background()
	if kind == "mongodb" {
		col := mg.Database("dbmix").Collection(table)
		if err := col.Drop(ctx); err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			if err := mg.Database("dbmix").CreateCollection(ctx, table); err != nil {
				t.Fatal(err)
			}
			return
		}
		docs := make([]any, count)
		for i := range count {
			docs[i] = bson.M{"_id": int64(i - 3), "name": "नमस्ते ' " + fmt.Sprint(i), "enabled": i%2 == 0, "score": float64(i) + 0.5, "created_at": time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC), "metadata": bson.M{"big": int64(9007199254740993), "items": bson.A{"x", true, nil}}, "payload": bson.Binary{Data: []byte{0, 1, 255}}, "optional": nil}
		}
		if _, err := col.InsertMany(ctx, docs); err != nil {
			t.Fatal(err)
		}
		return
	}
	db := pg
	ddl := "id BIGINT PRIMARY KEY,name TEXT NOT NULL,enabled BOOLEAN NOT NULL,score DOUBLE PRECISION NOT NULL,created_at TIMESTAMPTZ NOT NULL,metadata JSONB NOT NULL,payload BYTEA NOT NULL,optional TEXT"
	q := "INSERT INTO " + table + " VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
	if kind == "mysql" {
		db = my
		ddl = "id BIGINT PRIMARY KEY,name LONGTEXT NOT NULL,enabled BOOLEAN NOT NULL,score DOUBLE NOT NULL,created_at DATETIME(6) NOT NULL,metadata JSON NOT NULL,payload LONGBLOB NOT NULL,optional LONGTEXT"
		q = "INSERT INTO " + table + " VALUES (?,?,?,?,?,?,?,?)"
	}
	if _, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS "+table); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, "CREATE TABLE "+table+" ("+ddl+")"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	for i := range count {
		if _, err = tx.ExecContext(ctx, q, int64(i-3), "नमस्ते ' "+fmt.Sprint(i), i%2 == 0, float64(i)+0.5, time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC), `{"big":9007199254740993,"items":["x",true,null]}`, []byte{0, 1, 255}, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
}
func openPlan(t *testing.T, c config.Config) (connector.Source, connector.Destination, planner.Plan) {
	t.Helper()
	ctx := context.Background()
	s, d, err := connectors.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(); _ = d.Close() })
	p, err := planner.Build(ctx, c, s, d)
	if err != nil {
		t.Fatal(err)
	}
	return s, d, p
}
func run(t *testing.T, s connector.Source, d connector.Destination, p planner.Plan) *state.Migration {
	t.Helper()
	store, err := state.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	m := state.New(p)
	e := engine.Engine{Source: s, Destination: d, Save: store.Save, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err = e.Run(context.Background(), &m); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load(m.ID)
	if err != nil || loaded.Status != "completed" {
		t.Fatal(err)
	}
	return &m
}
func verify(t *testing.T, kind, table string, count int) {
	t.Helper()
	ctx := context.Background()
	if kind == "mongodb" {
		col := mg.Database("dbmix").Collection(table)
		n, err := col.CountDocuments(ctx, bson.D{})
		if err != nil || n != int64(count) {
			t.Fatalf("count %d: %v", n, err)
		}
		for i := range count {
			var doc bson.M
			if err = col.FindOne(ctx, bson.M{"_id": int64(i - 3)}).Decode(&doc); err != nil {
				t.Fatal(err)
			}
			if doc["name"] != "नमस्ते ' "+fmt.Sprint(i) || doc["enabled"] != (i%2 == 0) || doc["score"] != float64(i)+0.5 || doc["optional"] != nil {
				t.Fatalf("wrong document %v", doc)
			}
			metadata := bson.M{}
			for _, item := range doc["metadata"].(bson.D) {
				metadata[item.Key] = item.Value
			}
			if metadata["big"] != int64(9007199254740993) {
				t.Fatal("JSON integer precision lost")
			}
			if !reflect.DeepEqual(doc["payload"].(bson.Binary).Data, []byte{0, 1, 255}) {
				t.Fatal("binary changed")
			}
			if !doc["created_at"].(bson.DateTime).Time().Equal(time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC)) {
				t.Fatal("timestamp changed")
			}
		}
		return
	}
	db := pg
	if kind == "mysql" {
		db = my
	}
	rows, err := db.QueryContext(ctx, "SELECT id,name,enabled,score,created_at,metadata,payload,optional FROM "+table+" ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	n := 0
	for rows.Next() {
		var id int64
		var name string
		var enabled bool
		var score float64
		var stamp time.Time
		var raw, payload []byte
		var optional sql.NullString
		if err = rows.Scan(&id, &name, &enabled, &score, &stamp, &raw, &payload, &optional); err != nil {
			t.Fatal(err)
		}
		var meta map[string]json.RawMessage
		if err = json.Unmarshal(raw, &meta); err != nil {
			t.Fatal(err)
		}
		if id != int64(n-3) || name != "नमस्ते ' "+fmt.Sprint(n) || enabled != (n%2 == 0) || score != float64(n)+0.5 || optional.Valid || string(meta["big"]) != "9007199254740993" || !reflect.DeepEqual(payload, []byte{0, 1, 255}) || !stamp.Equal(time.Date(2024, 1, 2, 3, 4, 5, 123000000, time.UTC)) {
			t.Fatalf("row %d differs", n)
		}
		n++
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if n != count {
		t.Fatalf("count %d, want %d", n, count)
	}
}
func TestConnectorMatrix(t *testing.T) {
	kinds := []string{"postgres", "mysql", "mongodb"}
	for _, src := range kinds {
		seed(t, src, "matrix_source", 137)
	}
	for _, src := range kinds {
		for _, dst := range kinds {
			t.Run(src+"_to_"+dst, func(t *testing.T) {
				target := "matrix_" + src + "_" + dst
				c := cfg(t, src, dst, "matrix_source", target)
				s, d, p := openPlan(t, c)
				m := run(t, s, d, p)
				if m.Progress[0].Records != 137 {
					t.Fatal("bad progress count")
				}
				verify(t, dst, target, 137)
				run(t, s, d, p)
				verify(t, dst, target, 137)
			})
		}
	}
}
func TestEmptySource(t *testing.T) {
	for _, kind := range []string{"postgres", "mysql", "mongodb"} {
		t.Run(kind, func(t *testing.T) {
			seed(t, kind, "empty_source", 0)
			s, d, p := openPlan(t, cfg(t, kind, "postgres", "empty_source", "empty_"+kind))
			run(t, s, d, p)
			verify(t, "postgres", "empty_"+kind, 0)
		})
	}
}
