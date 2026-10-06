package connectors

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/model"
)

type sqlDB struct {
	assumeUTC bool
	db        *sql.DB
	dialect   string
}

func openSQL(ctx context.Context, e config.Endpoint, uri string, pool int) (*sqlDB, error) {
	var db *sql.DB
	if e.Type == "postgres" {
		u, err := url.Parse(uri)
		if err != nil || u.Scheme != "postgres" && u.Scheme != "postgresql" {
			return nil, fmt.Errorf("PostgreSQL connection must be a postgres:// URI")
		}
		mode := u.Query().Get("sslmode")
		if !e.AllowInsecure && mode != "verify-full" {
			return nil, fmt.Errorf("PostgreSQL requires sslmode=verify-full (or explicit allow_insecure for local development)")
		}
		cfg, err := pgx.ParseConfig(uri)
		if err != nil {
			return nil, fmt.Errorf("invalid PostgreSQL URI")
		}
		db = stdlib.OpenDB(*cfg)
	} else {
		cfg, err := mysql.ParseDSN(uri)
		if err != nil {
			return nil, fmt.Errorf("invalid MySQL DSN")
		}
		if !e.AllowInsecure && cfg.TLSConfig != "true" {
			return nil, fmt.Errorf("MySQL requires tls=true (or explicit allow_insecure for local development)")
		}
		if cfg.DBName == "" {
			return nil, fmt.Errorf("MySQL DSN requires a database")
		}
		cfg.ParseTime = true
		cfg.Loc = time.UTC
		cfg.MultiStatements = false
		cfg.AllowAllFiles = false
		cfg.InterpolateParams = false
		if cfg.Params == nil {
			cfg.Params = map[string]string{}
		}
		cfg.Params["time_zone"] = "'+00:00'"
		cfg.Params["sql_mode"] = "'STRICT_ALL_TABLES,NO_ZERO_DATE,NO_ZERO_IN_DATE,ERROR_FOR_DIVISION_BY_ZERO'"
		conn, err := mysql.NewConnector(cfg)
		if err != nil {
			return nil, fmt.Errorf("invalid MySQL configuration")
		}
		db = sql.OpenDB(conn)
	}
	db.SetMaxOpenConns(pool)
	db.SetMaxIdleConns(pool)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, safeError("ping", err)
	}
	return &sqlDB{db: db, dialect: e.Type, assumeUTC: e.AssumeUTC}, nil
}
func (s *sqlDB) Close() error { return s.db.Close() }
func (s *sqlDB) quote(x string) string {
	if s.dialect == "mysql" {
		return "`" + strings.ReplaceAll(x, "`", "``") + "`"
	}
	return `"` + strings.ReplaceAll(x, `"`, `""`) + `"`
}
func (s *sqlDB) table(x string) string {
	if s.dialect == "postgres" && !strings.Contains(x, ".") {
		x = "public." + x
	}
	p := strings.Split(x, ".")
	for i := range p {
		p[i] = s.quote(p[i])
	}
	return strings.Join(p, ".")
}
func (s *sqlDB) param(n int) string {
	if s.dialect == "postgres" {
		return "$" + strconv.Itoa(n)
	}
	return "?"
}
func (s *sqlDB) namespace(entity string) (string, string) {
	p := strings.Split(entity, ".")
	if len(p) == 2 {
		return p[0], p[1]
	}
	return "public", entity
}
func (s *sqlDB) Inspect(ctx context.Context, e config.Entity) (model.Inspection, error) {
	schema, exists, err := s.schema(ctx, e.Source)
	if err != nil {
		return model.Inspection{}, err
	}
	if !exists {
		return model.Inspection{}, fmt.Errorf("table %s does not exist", e.Source)
	}
	if s.dialect == "mysql" && !s.assumeUTC {
		cols, err := s.columns(ctx, e.Source)
		if err != nil {
			return model.Inspection{}, err
		}
		for _, col := range cols {
			if strings.HasPrefix(col.native, "datetime") {
				return model.Inspection{}, fmt.Errorf("MySQL DATETIME has no timezone; set source.assume_utc: true only when values represent UTC")
			}
		}
	}
	if schema.Key != e.Key {
		return model.Inspection{}, fmt.Errorf("%s must be the table's single-column primary key", e.Key)
	}
	f, _ := schema.Field(e.Key)
	if f.Type != model.Int64 {
		return model.Inspection{}, fmt.Errorf("SQL source primary key must be a signed integer")
	}
	var maxKey sql.NullInt64
	if err = s.db.QueryRowContext(ctx, "SELECT MAX("+s.quote(e.Key)+") FROM "+s.table(e.Source)).Scan(&maxKey); err != nil {
		return model.Inspection{}, safeError("read high-water key", err)
	}
	info := model.Inspection{Schema: schema, EstimatedRows: -1}
	if maxKey.Valid {
		info.Upper = model.Cursor(strconv.FormatInt(maxKey.Int64, 10))
	} else {
		info.EstimatedRows = 0
	}
	// Statistics are estimates; an exact COUNT(*) would scan every source during plan.
	ns, table := s.namespace(e.Source)
	if maxKey.Valid {
		var n sql.NullInt64
		if s.dialect == "postgres" {
			err = s.db.QueryRowContext(ctx, `SELECT GREATEST(c.reltuples,0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2`, ns, table).Scan(&n)
		} else {
			err = s.db.QueryRowContext(ctx, `SELECT TABLE_ROWS FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?`, table).Scan(&n)
		}
		if err != nil {
			return info, safeError("estimate source rows", err)
		}
		if n.Valid {
			info.EstimatedRows = n.Int64
		}
	}
	return info, nil
}
func (s *sqlDB) Read(ctx context.Context, entity string, schema model.Schema, after, upper model.Cursor, limit int) ([]model.Record, model.Cursor, error) {
	cols := make([]string, len(schema.Fields))
	for i, f := range schema.Fields {
		cols[i] = s.quote(f.Name)
	}
	args := []any{}
	where := ""
	key := s.quote(schema.Key)
	if after != "" {
		v, err := strconv.ParseInt(string(after), 10, 64)
		if err != nil {
			return nil, "", fmt.Errorf("invalid saved integer cursor")
		}
		args = append(args, v)
		where = key + ">" + s.param(len(args)) + " AND "
	}
	top, err := strconv.ParseInt(string(upper), 10, 64)
	if err != nil {
		return nil, "", fmt.Errorf("invalid high-water cursor")
	}
	args = append(args, top)
	where += key + "<=" + s.param(len(args))
	args = append(args, limit)
	q := "SELECT " + strings.Join(cols, ",") + " FROM " + s.table(entity) + " WHERE " + where + " ORDER BY " + key + " LIMIT " + s.param(len(args))
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, "", safeError("read rows", err)
	}
	defer func() { _ = rows.Close() }()
	out := make([]model.Record, 0, limit)
	cursor := after
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return nil, "", safeError("scan row", err)
		}
		r := model.Record{}
		for i, f := range schema.Fields {
			v, err := decodeSQL(values[i], f)
			if err != nil {
				return nil, "", err
			}
			r[f.Name] = v
		}
		cursor, err = model.Key(r[schema.Key], model.Int64)
		if err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	if err = rows.Err(); err != nil {
		return nil, "", safeError("read rows", err)
	}
	return out, cursor, nil
}
func decodeSQL(v any, f model.Field) (any, error) {
	if v == nil {
		return model.Normalize(nil, f)
	}
	switch f.Type {
	case model.String:
		if b, ok := v.([]byte); ok {
			v = string(b)
		}
	case model.JSON:
		switch x := v.(type) {
		case []byte:
			v = json.RawMessage(x)
		case string:
			v = json.RawMessage(x)
		}
	case model.Bool:
		switch x := v.(type) {
		case int64:
			if x != 0 && x != 1 {
				return nil, fmt.Errorf("field %s has a non-boolean integer", f.Name)
			}
			v = x == 1
		}
	case model.Float64:
		if b, ok := v.([]byte); ok {
			x, err := strconv.ParseFloat(string(b), 64)
			if err != nil {
				return nil, fmt.Errorf("invalid float field %s", f.Name)
			}
			v = x
		}
	}
	return model.Normalize(v, f)
}
