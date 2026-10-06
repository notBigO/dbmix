package connectors

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/model"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type mongoDB struct {
	client *mongo.Client
	db     *mongo.Database
}

func openMongo(ctx context.Context, e config.Endpoint, uri string, pool int) (*mongoDB, error) {
	journal := true
	opts := options.Client().ApplyURI(uri).SetMaxPoolSize(uint64(pool)).SetServerSelectionTimeout(10 * time.Second).SetWriteConcern(&writeconcern.WriteConcern{W: "majority", Journal: &journal}).SetReadConcern(readconcern.Majority())
	if err := opts.Validate(); err != nil {
		return nil, fmt.Errorf("invalid MongoDB URI/options")
	}
	if !e.AllowInsecure && (opts.TLSConfig == nil || opts.TLSConfig.InsecureSkipVerify) {
		return nil, fmt.Errorf("MongoDB requires verified TLS (or explicit allow_insecure for local development)")
	}
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, safeError("connect MongoDB", err)
	}
	if err = client.Ping(ctx, nil); err != nil {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = client.Disconnect(closeCtx)
		return nil, safeError("ping MongoDB", err)
	}
	return &mongoDB{client: client, db: client.Database(e.Database)}, nil
}
func (m *mongoDB) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return safeError("close MongoDB", m.client.Disconnect(ctx))
}
func (m *mongoDB) Inspect(ctx context.Context, e config.Entity) (model.Inspection, error) {
	info := model.Inspection{Schema: model.Schema{Key: e.Key}}
	if e.Key != "_id" {
		return info, fmt.Errorf("MongoDB source key must be _id")
	}
	for _, f := range e.Fields {
		info.Schema.Fields = append(info.Schema.Fields, model.Field{Name: f.Source, Type: f.Type, Nullable: f.Nullable})
	}
	key, ok := info.Schema.Field("_id")
	if !ok || key.Nullable || key.Type != model.Int64 && key.Type != model.ObjectID {
		return info, fmt.Errorf("MongoDB requires non-null _id declared as int64 or objectid")
	}
	names, err := m.db.ListCollectionNames(ctx, bson.M{"name": e.Source})
	if err != nil {
		return info, safeError("inspect collection", err)
	}
	if len(names) == 0 {
		return info, fmt.Errorf("source collection %s does not exist", e.Source)
	}
	col := m.db.Collection(e.Source)
	types := bson.A{"objectId"}
	if key.Type == model.Int64 {
		types = bson.A{"int", "long"}
	}
	// BSON sorts by type as well as value. Reject mixed key types before applying a
	// high-water predicate, which would otherwise silently omit another type bracket.
	err = col.FindOne(ctx, bson.M{"_id": bson.M{"$not": bson.M{"$type": types}}}, options.FindOne().SetProjection(bson.M{"_id": 1})).Err()
	if err == nil {
		return info, fmt.Errorf("collection contains _id values outside declared key type")
	}
	if err != mongo.ErrNoDocuments {
		return info, safeError("validate MongoDB key types", err)
	}
	var last bson.M
	err = col.FindOne(ctx, bson.D{}, options.FindOne().SetSort(bson.D{{Key: "_id", Value: -1}}).SetProjection(bson.M{"_id": 1})).Decode(&last)
	if err == mongo.ErrNoDocuments {
		return info, nil
	}
	if err != nil {
		return info, safeError("read MongoDB high-water key", err)
	}
	v, err := decodeMongo(last["_id"], key)
	if err != nil {
		return info, err
	}
	info.Upper, err = model.Key(v, key.Type)
	if err != nil {
		return info, err
	}
	info.EstimatedRows, err = col.EstimatedDocumentCount(ctx)
	return info, safeError("estimate documents", err)
}
func mongoCursor(c model.Cursor, kind model.Kind) (any, error) {
	if kind == model.ObjectID {
		v, err := bson.ObjectIDFromHex(string(c))
		if err != nil {
			return nil, fmt.Errorf("invalid saved ObjectID cursor")
		}
		return v, nil
	}
	v, err := strconv.ParseInt(string(c), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid saved integer cursor")
	}
	return v, nil
}
func (m *mongoDB) Read(ctx context.Context, entity string, schema model.Schema, after, upper model.Cursor, limit int) ([]model.Record, model.Cursor, error) {
	key, _ := schema.Field(schema.Key)
	top, err := mongoCursor(upper, key.Type)
	if err != nil {
		return nil, "", err
	}
	bounds := bson.M{"$lte": top}
	if after != "" {
		v, err := mongoCursor(after, key.Type)
		if err != nil {
			return nil, "", err
		}
		bounds["$gt"] = v
	}
	projection := bson.M{}
	for _, f := range schema.Fields {
		projection[f.Name] = 1
	}
	cur, err := m.db.Collection(entity).Find(ctx, bson.M{"_id": bounds}, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(int64(limit)).SetBatchSize(int32(limit)).SetProjection(projection))
	if err != nil {
		return nil, "", safeError("read documents", err)
	}
	defer func() { _ = cur.Close(ctx) }()
	out := make([]model.Record, 0, limit)
	next := after
	for cur.Next(ctx) {
		var doc bson.M
		if err = cur.Decode(&doc); err != nil {
			return nil, "", safeError("decode document", err)
		}
		r := model.Record{}
		for _, f := range schema.Fields {
			v, err := decodeMongo(doc[f.Name], f)
			if err != nil {
				return nil, "", err
			}
			r[f.Name] = v
		}
		next, err = model.Key(r[schema.Key], key.Type)
		if err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	return out, next, safeError("read documents", cur.Err())
}
func (m *mongoDB) Validate(ctx context.Context, entity string, schema model.Schema) error {
	if schema.Key != "_id" {
		return fmt.Errorf("MongoDB destination requires key mapped to _id")
	}
	key, _ := schema.Field(schema.Key)
	if key.Type != model.Int64 && key.Type != model.ObjectID {
		return fmt.Errorf("MongoDB _id must be int64 or objectid")
	}
	// Views, capped and time-series collections don't satisfy replacement-upsert semantics.
	cur, err := m.db.ListCollections(ctx, bson.M{"name": entity})
	if err != nil {
		return safeError("inspect destination collection", err)
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		var spec struct {
			Type    string `bson:"type"`
			Options bson.M `bson:"options"`
		}
		if err = cur.Decode(&spec); err != nil {
			return safeError("decode collection options", err)
		}
		if spec.Type != "collection" || spec.Options["timeseries"] != nil || spec.Options["capped"] == true {
			return fmt.Errorf("destination must be an ordinary uncapped collection")
		}
	}
	return safeError("inspect destination collection", cur.Err())
}
func (m *mongoDB) Prepare(ctx context.Context, entity string, schema model.Schema) error {
	if err := m.Validate(ctx, entity, schema); err != nil {
		return err
	}
	err := m.db.CreateCollection(ctx, entity)
	if err != nil {
		if command, ok := err.(mongo.CommandError); !ok || command.Code != 48 {
			return safeError("create collection", err)
		}
	}
	return m.Validate(ctx, entity, schema)
}
func (m *mongoDB) Write(ctx context.Context, entity string, schema model.Schema, rows []model.Record) error {
	writes := make([]mongo.WriteModel, 0, len(rows))
	for _, r := range rows {
		doc := bson.M{}
		for _, f := range schema.Fields {
			v, err := encodeMongo(r[f.Name], f)
			if err != nil {
				return err
			}
			doc[f.Name] = v
		}
		writes = append(writes, mongo.NewReplaceOneModel().SetFilter(bson.M{"_id": doc["_id"]}).SetReplacement(doc).SetUpsert(true))
	}
	if len(writes) == 0 {
		return nil
	}
	_, err := m.db.Collection(entity).BulkWrite(ctx, writes, options.BulkWrite().SetOrdered(true))
	return safeError("upsert documents", err)
}
