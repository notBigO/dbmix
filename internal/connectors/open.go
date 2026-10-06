// Package connectors wires supported database implementations to engine capabilities.
package connectors

import (
	"context"
	"fmt"
	"strings"

	"github.com/notBigO/dbmix/internal/config"
	"github.com/notBigO/dbmix/internal/connector"
)

type database interface {
	connector.Source
	connector.Destination
}

func open(ctx context.Context, e config.Endpoint, pool int) (database, error) {
	uri, err := e.Resolve()
	if err != nil {
		return nil, err
	}
	switch e.Type {
	case "postgres", "mysql":
		return openSQL(ctx, e, uri, pool)
	case "mongodb":
		return openMongo(ctx, e, uri, pool)
	default:
		return nil, fmt.Errorf("unsupported connector %s", e.Type)
	}
}
func Open(ctx context.Context, c config.Config) (connector.Source, connector.Destination, error) {
	// Refuse obvious self-copies; users must also avoid aliases to the same physical entity.
	a, err := c.Source.Resolve()
	if err != nil {
		return nil, nil, err
	}
	b, err := c.Destination.Resolve()
	if err != nil {
		return nil, nil, err
	}
	if c.Source.Type == c.Destination.Type && a == b && c.Source.Database == c.Destination.Database {
		for _, s := range c.Entities {
			for _, d := range c.Entities {
				sourceName, targetName := s.Source, d.Target
				if c.Source.Type == "postgres" {
					sourceName = strings.TrimPrefix(sourceName, "public.")
					targetName = strings.TrimPrefix(targetName, "public.")
				}
				if sourceName == targetName {
					return nil, nil, fmt.Errorf("source and destination entities overlap")
				}
			}
		}
	}
	for _, e := range c.Entities {
		if c.Source.Type != "postgres" && strings.Contains(e.Source, ".") {
			return nil, nil, fmt.Errorf("%s source requires an unqualified entity name", c.Source.Type)
		}
		if c.Destination.Type != "postgres" && strings.Contains(e.Target, ".") {
			return nil, nil, fmt.Errorf("%s destination requires an unqualified entity name", c.Destination.Type)
		}
	}
	src, err := open(ctx, c.Source, 2)
	if err != nil {
		return nil, nil, fmt.Errorf("connect source %s: %w", c.Source.Type, err)
	}
	dst, err := open(ctx, c.Destination, c.Workers+2)
	if err != nil {
		_ = src.Close()
		return nil, nil, fmt.Errorf("connect destination %s: %w", c.Destination.Type, err)
	}
	return src, dst, nil
}
