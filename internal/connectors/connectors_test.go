package connectors

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/model"
	"github.com/notBigO/dbmix/internal/retry"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestNativeTypes(t *testing.T) {
	for _, tt := range []struct {
		db, native string
		kind       model.Kind
		bad        bool
	}{{"postgres", "int8", model.Int64, false}, {"postgres", "numeric", "", true}, {"postgres", "timestamp", "", true}, {"postgres", "_int4", "", true}, {"mysql", "bigint unsigned", "", true}, {"mysql", "tinyint(1)", model.Bool, false}, {"mysql", "decimal(10,2)", "", true}, {"mysql", "json", model.JSON, false}} {
		k, err := nativeKind(tt.db, tt.native)
		if k != tt.kind || (err != nil) != tt.bad {
			t.Fatalf("%+v => %s %v", tt, k, err)
		}
	}
}
func TestSafeErrors(t *testing.T) {
	for _, tt := range []struct {
		err   error
		retry bool
	}{{&pgconn.PgError{Code: "40001", Message: "password=SECRET"}, true}, {&pgconn.PgError{Code: "23505", Detail: "SECRET"}, false}, {&mysql.MySQLError{Number: 1213, Message: "SECRET"}, true}, {&mysql.MySQLError{Number: 1062, Message: "SECRET"}, false}, {mongo.BulkWriteException{Labels: []string{"RetryableWriteError"}}, true}, {errors.New("postgres://user:SECRET@host"), false}} {
		err := safeError("write", tt.err)
		if strings.Contains(err.Error(), "SECRET") || retry.Transient(err) != tt.retry {
			t.Fatal(err)
		}
	}
	if !errors.Is(safeError("read", context.Canceled), context.Canceled) {
		t.Fatal("lost cancellation")
	}
}
func TestMongoPrecision(t *testing.T) {
	f := model.Field{Name: "created_at", Type: model.Time}
	if _, err := encodeMongo(time.Unix(0, 123456000), f); err == nil {
		t.Fatal("timestamp rounded")
	}
	if _, err := encodeMongo(time.Unix(0, 123000000), f); err != nil {
		t.Fatal(err)
	}
	if _, err := decodeMongo(bson.D{{Key: "nested", Value: bson.NewObjectID()}}, model.Field{Name: "json", Type: model.JSON}); err == nil {
		t.Fatal("nested BSON silently coerced")
	}
	out, err := jsonToBSON(json.RawMessage(`{"big":9007199254740993,"$date":"literal"}`), "meta")
	if err != nil {
		t.Fatal(err)
	}
	if out.(bson.M)["big"] != int64(9007199254740993) || out.(bson.M)["$date"] != "literal" {
		t.Fatal("JSON changed")
	}
	if _, err = jsonToBSON(json.RawMessage(`9223372036854775808`), "meta"); err == nil {
		t.Fatal("integer overflow")
	}
}
func TestTLSRequired(t *testing.T) {
	for _, kind := range []string{"postgres", "mysql", "mongodb"} {
		t.Run(kind, func(t *testing.T) {
			uri := map[string]string{"postgres": "postgres://u:p@localhost/db?sslmode=disable", "mysql": "u:p@tcp(localhost:3306)/db", "mongodb": "mongodb://localhost:27017"}[kind]
			t.Setenv("DBMIX_TLS_TEST", uri)
			_, err := open(context.Background(), config.Endpoint{Type: kind, URI: "${DBMIX_TLS_TEST}", Database: "db"}, 1)
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "requires") {
				t.Fatal(err)
			}
		})
	}
}
func TestIdentifierQuoting(t *testing.T) {
	s := sqlDB{dialect: "postgres"}
	if s.quote(`a"b`) != `"a""b"` {
		t.Fatal("unsafe postgres quote")
	}
	s.dialect = "mysql"
	if s.quote("a`b") != "`a``b`" {
		t.Fatal("unsafe mysql quote")
	}
}
