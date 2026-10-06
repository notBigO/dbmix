package planner

import (
	"context"
	"testing"

	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/model"
)

type source struct{ info model.Inspection }

func (s source) Inspect(context.Context, config.Entity) (model.Inspection, error) { return s.info, nil }
func (source) Read(context.Context, string, model.Schema, model.Cursor, model.Cursor, int) ([]model.Record, model.Cursor, error) {
	panic("planner must not read data")
}
func (source) Close() error { return nil }

type destination struct{ validations int }

func (d *destination) Validate(context.Context, string, model.Schema) error {
	d.validations++
	return nil
}
func (*destination) Prepare(context.Context, string, model.Schema) error {
	panic("plan mutated destination")
}
func (*destination) Write(context.Context, string, model.Schema, []model.Record) error {
	panic("plan wrote records")
}
func (*destination) Close() error { return nil }
func TestMap(t *testing.T) {
	info := model.Inspection{Schema: model.Schema{Key: "id", Fields: []model.Field{{Name: "id", Type: model.Int64}, {Name: "email", Type: model.String}}}}
	for _, tt := range []struct {
		name   string
		fields []config.Field
		bad    bool
	}{{"infer", nil, false}, {"rename", []config.Field{{Source: "id", Target: "_id"}}, false}, {"drop key", []config.Field{{Source: "email", Target: "email"}}, true}, {"coerce", []config.Field{{Source: "id", Target: "id", Type: model.Float64}}, true}, {"missing", []config.Field{{Source: "nope", Target: "nope"}}, true}} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Map(config.Entity{Source: "users", Target: "users", Key: "id", Fields: tt.fields}, info)
			if (err != nil) != tt.bad {
				t.Fatal(err)
			}
		})
	}
}
func TestReadOnlyPlan(t *testing.T) {
	c, err := config.Parse([]byte("version: 1\nsource: {type: postgres, uri: '${SRC}'}\ndestination: {type: mysql, uri: '${DST}'}\nsource_quiescent: true\nentities: [{source: users, target: users, key: id}]\n"))
	if err != nil {
		t.Fatal(err)
	}
	d := &destination{}
	s := source{model.Inspection{Schema: model.Schema{Key: "id", Fields: []model.Field{{Name: "id", Type: model.Int64}}}, EstimatedRows: 1100}}
	p, err := Build(context.Background(), c, s, d)
	if err != nil || d.validations != 1 || p.Entities[0].EstimatedBatches != 2 {
		t.Fatalf("%+v %v", p, err)
	}
	s.info.Schema.Fields = append([]model.Field(nil), s.info.Schema.Fields...)
	s.info.Schema.Fields[0].Nullable = true
	if err = Verify(context.Background(), p, s, d); err == nil {
		t.Fatal("schema drift accepted")
	}
}
