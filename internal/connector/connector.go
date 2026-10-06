// Package connector defines capabilities, not database pairs.
package connector

import (
	"context"
	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/model"
)

type Source interface {
	Inspect(context.Context, config.Entity) (model.Inspection, error)
	Read(context.Context, string, model.Schema, model.Cursor, model.Cursor, int) ([]model.Record, model.Cursor, error)
	Close() error
}
type Destination interface {
	// Validate is read-only. Prepare may create a missing entity, never alter one.
	Validate(context.Context, string, model.Schema) error
	Prepare(context.Context, string, model.Schema) error
	// Write must be concurrency-safe and replay-safe, acknowledging durable writes only.
	Write(context.Context, string, model.Schema, []model.Record) error
	Close() error
}
