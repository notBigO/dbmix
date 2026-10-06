package planner

import (
	"context"
	"fmt"
	"reflect"

	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/connector"
	"github.com/notBigO/dbmix/internal/model"
)

type Entity struct {
	Source           string             `json:"source"`
	Target           string             `json:"target"`
	Input            model.Schema       `json:"input"`
	Output           model.Schema       `json:"output"`
	Fields           []model.Projection `json:"fields"`
	Upper            model.Cursor       `json:"upper"`
	EstimatedRows    int64              `json:"estimated_rows"`
	EstimatedBatches int64              `json:"estimated_batches"`
}
type Plan struct {
	Config   config.Config `json:"config"`
	Entities []Entity      `json:"entities"`
	Warnings []string      `json:"warnings"`
}

func Build(ctx context.Context, c config.Config, s connector.Source, d connector.Destination) (Plan, error) {
	p := Plan{Config: c, Warnings: []string{"Source must remain quiescent through completion and resume; high-water keys are not snapshots.", "Upsert replays are possible; destination triggers must tolerate repeated writes.", "Indexes, foreign keys, defaults, sequences and deletes are not migrated."}}
	if err := c.Validate(); err != nil {
		return p, err
	}
	for _, e := range c.Entities {
		info, err := s.Inspect(ctx, e)
		if err != nil {
			return p, fmt.Errorf("inspect source %s: %w", e.Source, err)
		}
		ep, err := Map(e, info)
		if err != nil {
			return p, err
		}
		ep.EstimatedBatches = -1
		if ep.EstimatedRows >= 0 {
			ep.EstimatedBatches = (ep.EstimatedRows + int64(c.BatchSize) - 1) / int64(c.BatchSize)
		}
		if err = d.Validate(ctx, e.Target, ep.Output); err != nil {
			return p, fmt.Errorf("validate destination %s: %w", e.Target, err)
		}
		p.Entities = append(p.Entities, ep)
	}
	return p, nil
}
func Map(e config.Entity, i model.Inspection) (Entity, error) {
	p := Entity{Source: e.Source, Target: e.Target, Input: i.Schema, Upper: i.Upper, EstimatedRows: i.EstimatedRows}
	key, ok := i.Schema.Field(e.Key)
	if !ok || i.Schema.Key != e.Key || key.Nullable || (key.Type != model.Int64 && key.Type != model.ObjectID) {
		return p, fmt.Errorf("%s requires a single non-null int64 primary key or MongoDB ObjectID _id", e.Source)
	}
	fields := e.Fields
	if len(fields) == 0 {
		for _, f := range i.Schema.Fields {
			fields = append(fields, config.Field{Source: f.Name, Target: f.Name})
		}
	}
	for _, f := range fields {
		if !config.Identifier(f.Source) || !config.Identifier(f.Target) {
			return p, fmt.Errorf("discovered field requires preprocessing: unsupported identifier")
		}
		src, ok := i.Schema.Field(f.Source)
		if !ok {
			return p, fmt.Errorf("source field %s not found", f.Source)
		}
		if f.Type != "" && f.Type != src.Type {
			return p, fmt.Errorf("field %s: implicit conversion from %s to %s is unsupported", f.Source, src.Type, f.Type)
		}
		target := src
		target.Name = f.Target
		p.Fields = append(p.Fields, model.Projection{Source: f.Source, Target: target})
		p.Output.Fields = append(p.Output.Fields, target)
		if f.Source == e.Key {
			p.Output.Key = f.Target
		}
	}
	if p.Output.Key == "" {
		return p, fmt.Errorf("mapping must preserve primary key %s", e.Key)
	}
	return p, nil
}

// Verify prevents schema drift on resume. The original high-water boundary is retained.
func Verify(ctx context.Context, p Plan, s connector.Source, d connector.Destination) error {
	for n, e := range p.Entities {
		i, err := s.Inspect(ctx, p.Config.Entities[n])
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(i.Schema, e.Input) {
			return fmt.Errorf("source schema changed for %s; create a new migration", e.Source)
		}
		if err = d.Validate(ctx, e.Target, e.Output); err != nil {
			return err
		}
	}
	return nil
}
