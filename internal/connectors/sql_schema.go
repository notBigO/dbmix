package connectors

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/notBigO/dbmix/internal/model"
)

type column struct {
	name, native, nullable, key, generated string
	precision                              sql.NullInt64
}

func (s *sqlDB) columns(ctx context.Context, entity string) ([]column, error) {
	ns, name := s.namespace(entity)
	var q string
	var args []any
	if s.dialect == "postgres" {
		q = `SELECT c.column_name,c.udt_name,c.is_nullable,
  CASE WHEN EXISTS (SELECT 1 FROM information_schema.table_constraints t JOIN information_schema.key_column_usage k ON t.constraint_name=k.constraint_name AND t.constraint_schema=k.constraint_schema AND t.table_name=k.table_name WHERE t.constraint_type='PRIMARY KEY' AND t.table_schema=c.table_schema AND t.table_name=c.table_name AND k.column_name=c.column_name) THEN 'PRI' ELSE '' END,
  CASE WHEN c.is_generated='ALWAYS' OR c.is_identity='YES' THEN 'generated' ELSE '' END,c.datetime_precision
  FROM information_schema.columns c WHERE c.table_schema=$1 AND c.table_name=$2 ORDER BY c.ordinal_position`
		args = []any{ns, name}
	} else {
		q = `SELECT COLUMN_NAME,COLUMN_TYPE,IS_NULLABLE,COLUMN_KEY,EXTRA,DATETIME_PRECISION FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? ORDER BY ORDINAL_POSITION`
		args = []any{name}
	}
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, safeError("discover columns", err)
	}
	defer func() { _ = rows.Close() }()
	var out []column
	for rows.Next() {
		var c column
		if err = rows.Scan(&c.name, &c.native, &c.nullable, &c.key, &c.generated, &c.precision); err != nil {
			return nil, safeError("scan columns", err)
		}
		out = append(out, c)
	}
	return out, safeError("discover columns", rows.Err())
}
func nativeKind(dialect, native string) (model.Kind, error) {
	if dialect == "postgres" {
		switch native {
		case "int2", "int4", "int8":
			return model.Int64, nil
		case "float4", "float8":
			return model.Float64, nil
		case "text", "varchar", "bpchar":
			return model.String, nil
		case "bool":
			return model.Bool, nil
		case "timestamptz":
			return model.Time, nil
		case "json", "jsonb":
			return model.JSON, nil
		case "bytea":
			return model.Bytes, nil
		}
	}
	if dialect == "mysql" {
		if strings.Contains(native, "unsigned") {
			return "", fmt.Errorf("unsigned integers need explicit preprocessing")
		}
		base := strings.Split(native, "(")[0]
		switch base {
		case "tinyint":
			if native == "tinyint(1)" {
				return model.Bool, nil
			}
			return model.Int64, nil
		case "smallint", "mediumint", "int", "bigint":
			return model.Int64, nil
		case "float", "double":
			return model.Float64, nil
		case "char", "varchar", "tinytext", "text", "mediumtext", "longtext":
			return model.String, nil
		case "datetime", "timestamp":
			return model.Time, nil
		case "json":
			return model.JSON, nil
		case "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob":
			return model.Bytes, nil
		}
	}
	return "", fmt.Errorf("unsupported native type %s; preprocess it into a supported type", native)
}
func (s *sqlDB) schema(ctx context.Context, entity string) (model.Schema, bool, error) {
	cols, err := s.columns(ctx, entity)
	if err != nil {
		return model.Schema{}, false, err
	}
	out := model.Schema{}
	keys := 0
	for _, c := range cols {
		kind, err := nativeKind(s.dialect, c.native)
		if err != nil {
			return out, true, fmt.Errorf("column %s: %w", c.name, err)
		}
		out.Fields = append(out.Fields, model.Field{Name: c.name, Type: kind, Nullable: c.nullable == "YES"})
		if c.key == "PRI" {
			out.Key = c.name
			keys++
		}
	}
	if keys != 1 {
		out.Key = ""
	}
	return out, len(cols) > 0, nil
}
func (s *sqlDB) ddlType(f model.Field) string {
	if s.dialect == "postgres" {
		switch f.Type {
		case model.Int64:
			return "BIGINT"
		case model.Float64:
			return "DOUBLE PRECISION"
		case model.String, model.ObjectID:
			return "TEXT"
		case model.Bool:
			return "BOOLEAN"
		case model.Time:
			return "TIMESTAMPTZ"
		case model.JSON:
			return "JSONB"
		case model.Bytes:
			return "BYTEA"
		}
	}
	switch f.Type {
	case model.Int64:
		return "BIGINT"
	case model.Float64:
		return "DOUBLE"
	case model.String:
		return "LONGTEXT"
	case model.ObjectID:
		return "VARCHAR(24) COLLATE utf8mb4_bin"
	case model.Bool:
		return "BOOLEAN"
	case model.Time:
		return "DATETIME(6)"
	case model.JSON:
		return "JSON"
	case model.Bytes:
		return "LONGBLOB"
	}
	return ""
}
func (s *sqlDB) Validate(ctx context.Context, entity string, want model.Schema) error {
	for _, f := range want.Fields {
		if s.ddlType(f) == "" {
			return fmt.Errorf("unsupported destination type %s", f.Type)
		}
	}
	cols, err := s.columns(ctx, entity)
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		return nil
	}
	if len(cols) != len(want.Fields) {
		return fmt.Errorf("destination %s must have exactly the planned columns", entity)
	}
	keys := 0
	for _, c := range cols {
		f, ok := want.Field(c.name)
		if !ok {
			return fmt.Errorf("unexpected destination column %s", c.name)
		}
		if c.nullable == "YES" != f.Nullable {
			return fmt.Errorf("destination nullability differs for %s", c.name)
		}
		if strings.Contains(strings.ToLower(c.generated), "generated") {
			return fmt.Errorf("generated destination columns are unsupported")
		}
		if c.key == "PRI" {
			keys++
			if c.name != want.Key {
				return fmt.Errorf("destination primary key differs")
			}
		}
		if f.Type == model.Time && (!c.precision.Valid || c.precision.Int64 != 6) {
			return fmt.Errorf("destination column %s requires microsecond timestamp precision", c.name)
		}
		if !s.compatible(c.native, f) {
			return fmt.Errorf("destination column %s must use canonical type %s (found %s)", c.name, s.ddlType(f), c.native)
		}
	}
	if keys != 1 {
		return fmt.Errorf("destination requires exactly the planned single-column primary key")
	}
	if s.dialect == "mysql" {
		var engine string
		if err = s.db.QueryRowContext(ctx, `SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=?`, entity).Scan(&engine); err != nil {
			return safeError("inspect storage engine", err)
		}
		if engine != "InnoDB" {
			return fmt.Errorf("MySQL destination requires InnoDB for atomic batches")
		}
		var n int
		if err = s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=? AND NON_UNIQUE=0 AND INDEX_NAME<>'PRIMARY'`, entity).Scan(&n); err != nil {
			return safeError("inspect unique indexes", err)
		}
		if n != 0 {
			return fmt.Errorf("MySQL destination cannot have additional unique indexes: upsert could match the wrong row")
		}
	}
	return nil
}
func (s *sqlDB) compatible(native string, f model.Field) bool {
	if s.dialect == "postgres" {
		expected := map[model.Kind]string{model.Int64: "int8", model.Float64: "float8", model.String: "text", model.ObjectID: "text", model.Bool: "bool", model.Time: "timestamptz", model.JSON: "jsonb", model.Bytes: "bytea"}
		return native == expected[f.Type]
	}
	expected := map[model.Kind]string{model.Int64: "bigint", model.Float64: "double", model.String: "longtext", model.ObjectID: "varchar(24)", model.Bool: "tinyint(1)", model.Time: "datetime(6)", model.JSON: "json", model.Bytes: "longblob"}
	return native == expected[f.Type]
}
func (s *sqlDB) Prepare(ctx context.Context, entity string, schema model.Schema) error {
	if err := s.Validate(ctx, entity, schema); err != nil {
		return err
	}
	existing, err := s.columns(ctx, entity)
	if err != nil {
		return err
	}
	if len(existing) > 0 {
		return nil
	}
	defs := make([]string, 0, len(schema.Fields)+1)
	for _, f := range schema.Fields {
		d := s.quote(f.Name) + " " + s.ddlType(f)
		if !f.Nullable {
			d += " NOT NULL"
		}
		defs = append(defs, d)
	}
	defs = append(defs, "PRIMARY KEY ("+s.quote(schema.Key)+")")
	suffix := ""
	if s.dialect == "mysql" {
		suffix = " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"
	}
	_, err = s.db.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS "+s.table(entity)+" ("+strings.Join(defs, ",")+")"+suffix)
	if err != nil {
		return safeError("create destination table", err)
	}
	return s.Validate(ctx, entity, schema)
}
