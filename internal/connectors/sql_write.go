package connectors

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/notBigO/dbmix/internal/model"
)

func (s *sqlDB) Write(ctx context.Context, entity string, schema model.Schema, records []model.Record) error {
	cols, params, updates := []string{}, []string{}, []string{}
	for i, f := range schema.Fields {
		col := s.quote(f.Name)
		cols = append(cols, col)
		params = append(params, s.param(i+1))
		if f.Name != schema.Key {
			if s.dialect == "postgres" {
				updates = append(updates, col+"=EXCLUDED."+col)
			} else {
				updates = append(updates, col+"=VALUES("+col+")")
			}
		}
	}
	q := "INSERT INTO " + s.table(entity) + " (" + strings.Join(cols, ",") + ") VALUES (" + strings.Join(params, ",") + ")"
	if s.dialect == "postgres" {
		q += " ON CONFLICT (" + s.quote(schema.Key) + ") "
		if len(updates) == 0 {
			q += "DO NOTHING"
		} else {
			q += "DO UPDATE SET " + strings.Join(updates, ",")
		}
	} else {
		if len(updates) == 0 {
			updates = append(updates, s.quote(schema.Key)+"=VALUES("+s.quote(schema.Key)+")")
		}
		q += " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ",")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return safeError("begin batch", err)
	}
	defer func() { _ = tx.Rollback() }()
	stmt, err := tx.PrepareContext(ctx, q)
	if err != nil {
		return safeError("prepare upsert", err)
	}
	defer stmt.Close()
	for _, r := range records {
		args := make([]any, len(schema.Fields))
		for i, f := range schema.Fields {
			v := r[f.Name]
			if raw, ok := v.(json.RawMessage); ok {
				v = string(raw)
			}
			if t, ok := v.(time.Time); ok {
				if t.Nanosecond()%1000 != 0 {
					return fmt.Errorf("field %s exceeds SQL microsecond precision", f.Name)
				}
				if s.dialect == "mysql" && (t.Year() < 1000 || t.Year() > 9999) {
					return fmt.Errorf("field %s is outside MySQL datetime range", f.Name)
				}
			}
			args[i] = v
		}
		if _, err = stmt.ExecContext(ctx, args...); err != nil {
			return safeError("upsert row", err)
		}
	}
	return safeError("commit batch", tx.Commit())
}
