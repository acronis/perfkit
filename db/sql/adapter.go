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

func (d *sqlQuerier) ping(ctx context.Context) error {
	return d.be.PingContext(ctx)
}
func (d *sqlQuerier) stats() sql.DBStats {
	return d.be.Stats()
}
func (d *sqlQuerier) rawSession() interface{} {
	return d.be
}
func (d *sqlQuerier) close() error {
	return d.be.Close()
}
func (d *sqlQuerier) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.be.ExecContext(ctx, query, args...)
}
func (d *sqlQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.be.QueryRowContext(ctx, query, args...)
}
func (d *sqlQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.be.QueryContext(ctx, query, args...)
}
func (d *sqlQuerier) prepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return d.be.PrepareContext(ctx, query)
}
func (d *sqlQuerier) begin(ctx context.Context) (transaction, error) {
	be, err := d.be.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &sqlTransaction{be}, nil
}

func (t *sqlTransaction) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.be.ExecContext(ctx, query, args...)
}
func (t *sqlTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.be.QueryRowContext(ctx, query, args...)
}
func (t *sqlTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.be.QueryContext(ctx, query, args...)
}
func (t *sqlTransaction) prepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return t.be.PrepareContext(ctx, query)
}
func (t *sqlTransaction) commit() error {
	return t.be.Commit()
}
func (t *sqlTransaction) rollback() error {
	return t.be.Rollback()
}

type sqlSurrogateResult struct{}

func (r *sqlSurrogateResult) LastInsertId() (int64, error) {
	return 0, nil
}
func (r *sqlSurrogateResult) RowsAffected() (int64, error) {
	return 0, nil
}
