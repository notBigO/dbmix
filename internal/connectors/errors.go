package connectors

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Database messages can contain connection strings, SQL values, or document contents.
// Keep the machine code and error class, never server-provided detail, in public errors.
type dbError struct {
	op, code  string
	transient bool
}

func (e *dbError) Error() string   { return fmt.Sprintf("%s: database error %s", e.op, e.code) }
func (e *dbError) Temporary() bool { return e.transient }
func safeError(op string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: %w", op, context.Canceled)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%s: %w", op, context.DeadlineExceeded)
	}
	var pg *pgconn.PgError
	if errors.As(err, &pg) {
		return &dbError{op, pg.Code, pg.Code == "40001" || pg.Code == "40P01" || pg.Code == "53300" || pg.Code == "57P01" || len(pg.Code) >= 2 && pg.Code[:2] == "08"}
	}
	var my *mysql.MySQLError
	if errors.As(err, &my) {
		return &dbError{op, fmt.Sprint(my.Number), my.Number == 1205 || my.Number == 1213 || my.Number == 1040 || my.Number == 2006 || my.Number == 2013}
	}
	var command mongo.CommandError
	if errors.As(err, &command) {
		return &dbError{op, fmt.Sprint(command.Code), command.HasErrorLabel("RetryableWriteError") || command.Code == 91 || command.Code == 11600}
	}
	var network net.Error
	if errors.As(err, &network) {
		return &dbError{op, "network", true}
	}
	if mongo.IsNetworkError(err) || errors.Is(err, io.EOF) {
		return &dbError{op, "connection_lost", true}
	}
	if mongo.IsDuplicateKeyError(err) {
		return &dbError{op, "duplicate_key", false}
	}
	return &dbError{op, "operation_failed (check server logs)", false}
}
