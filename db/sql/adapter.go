package sql

import (
	"context"
	"database/sql"
)

type sqlQuerier struct {
	be *sql.DB
}
type sqlTransaction struct {
	be *sql.Tx
}

func (d *sqlQuerier) Ping(ctx context.Context) error {
	return d.be.PingContext(ctx)
}
func (d *sqlQuerier) Stats() sql.DBStats {
	return d.be.Stats()
}
func (d *sqlQuerier) RawSession() interface{} {
	return d.be
}
func (d *sqlQuerier) Close() error {
	return d.be.Close()
}
func (d *sqlQuerier) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.be.ExecContext(ctx, query, args...)
}
func (d *sqlQuerier) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.be.QueryRowContext(ctx, query, args...)
}
func (d *sqlQuerier) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.be.QueryContext(ctx, query, args...)
}
func (d *sqlQuerier) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.be.PrepareContext(ctx, query)
}
func (d *sqlQuerier) Begin(ctx context.Context) (transaction, error) {
	be, err := d.be.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &sqlTransaction{be}, nil
}

func (t *sqlTransaction) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.be.ExecContext(ctx, query, args...)
}
func (t *sqlTransaction) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.be.QueryRowContext(ctx, query, args...)
}
func (t *sqlTransaction) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.be.QueryContext(ctx, query, args...)
}
func (t *sqlTransaction) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return t.be.PrepareContext(ctx, query)
}
func (t *sqlTransaction) Commit() error {
	return t.be.Commit()
}
func (t *sqlTransaction) Rollback() error {
	return t.be.Rollback()
}
