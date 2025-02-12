package sql

import (
	"context"
	"database/sql"
)

// sqlQuerier wraps a standard sql.DB connection
type sqlQuerier struct {
	be *sql.DB // backend database connection
}

// sqlTransaction wraps a standard sql.Tx transaction
type sqlTransaction struct {
	be *sql.Tx // backend transaction
}

// ping checks if database connection is alive
func (d *sqlQuerier) ping(ctx context.Context) error {
	return d.be.PingContext(ctx)
}

// stats returns database connection statistics
func (d *sqlQuerier) stats() sql.DBStats {
	return d.be.Stats()
}

// rawSession returns the underlying database connection
func (d *sqlQuerier) rawSession() interface{} {
	return d.be
}

// close terminates the database connection
func (d *sqlQuerier) close() error {
	return d.be.Close()
}

// execContext executes a SQL query without returning rows
// Used for INSERT, UPDATE, DELETE operations
func (d *sqlQuerier) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.be.ExecContext(ctx, query, args...)
}

// queryRowContext executes a query expecting a single row result
// Typically used for SELECT queries returning one row
func (d *sqlQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return d.be.QueryRowContext(ctx, query, args...)
}

// queryContext executes a query that can return multiple rows
// Used for SELECT queries potentially returning multiple results
func (d *sqlQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.be.QueryContext(ctx, query, args...)
}

// prepareContext creates a prepared statement for later execution
// Helps prevent SQL injection and improves performance for repeated queries
func (d *sqlQuerier) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	var stmt, err = d.be.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &sqlStmt{stmt}, nil
}

// begin starts a new database transaction
func (d *sqlQuerier) begin(ctx context.Context) (transaction, error) {
	be, err := d.be.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &sqlTransaction{be}, nil
}

// Transaction methods implementing the querier interface

// execContext executes a SQL query within a transaction
func (t *sqlTransaction) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.be.ExecContext(ctx, query, args...)
}

// queryRowContext executes a single-row query within a transaction
func (t *sqlTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return t.be.QueryRowContext(ctx, query, args...)
}

// queryContext executes a multi-row query within a transaction
func (t *sqlTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.be.QueryContext(ctx, query, args...)
}

// prepareContext creates a prepared statement within a transaction
func (t *sqlTransaction) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	var stmt, err = t.be.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}

	return &sqlStmt{stmt}, nil
}

// commit finalizes the transaction, making changes permanent
func (t *sqlTransaction) commit() error {
	return t.be.Commit()
}

// rollback aborts the transaction, reverting all changes
func (t *sqlTransaction) rollback() error {
	return t.be.Rollback()
}

// sqlSurrogateResult provides a no-op implementation of sql.Result
// Useful for cases where result information isn't needed or available
type sqlSurrogateResult struct{}

// LastInsertId returns 0 and nil as this is a surrogate implementation
func (r *sqlSurrogateResult) LastInsertId() (int64, error) {
	return 0, nil
}

// RowsAffected returns 0 and nil as this is a surrogate implementation
func (r *sqlSurrogateResult) RowsAffected() (int64, error) {
	return 0, nil
}
