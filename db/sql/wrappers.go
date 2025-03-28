package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.uber.org/atomic"

	"github.com/acronis/perfkit/db"
)

/*
Package sql provides SQL database adapters with comprehensive logging and dry-run capabilities.

This file contains all wrapper implementations that handle:
- Dry-run mode: simulates SQL operations without actually executing them
- Query logging: logs all SQL queries, preparations, and executions
- Row result logging: logs the content of returned rows (limited to maxRowsToPrint rows)
- Performance measurements: tracks time spent in various database operations

The wrappers in this file decorate the core SQL functionality with these features
while maintaining the original interfaces.
*/

const maxRowsToPrint = 10

// wrappedRow is a struct for storing and logging DB *sql.Row results
type wrappedRow struct {
	row *sql.Row

	readRowsLogger db.Logger
}

// Scan copies the columns in the current row into the values pointed at by dest.
// Logs the scanned values if readRowsLogger is configured.
func (r *wrappedRow) Scan(dest ...any) error {
	var err = r.row.Scan(dest...)

	if r.readRowsLogger != nil {
		// Create a single log line with all columns
		var values = db.DumpRecursive(dest, " ")
		r.readRowsLogger.Log(fmt.Sprintf("Row: %s", values))
	}

	return err
}

// wrappedRows is a struct for storing and logging DB *sql.Rows results
type wrappedRows struct {
	rows *sql.Rows

	readRowsLogger db.Logger
	printed        int
}

// Next advances the cursor to the next row, returning false if no more rows
func (r *wrappedRows) Next() bool {
	return r.rows.Next()
}

// Err returns any error that was encountered during iteration
func (r *wrappedRows) Err() error {
	return r.rows.Err()
}

// Scan copies the columns in the current row into the values pointed at by dest.
// Logs the scanned values if readRowsLogger is configured, up to maxRowsToPrint rows.
func (r *wrappedRows) Scan(dest ...interface{}) error {
	var err = r.rows.Scan(dest...)

	if r.readRowsLogger != nil {
		if r.printed >= maxRowsToPrint {
			return err
		} else if r.printed == maxRowsToPrint {
			r.readRowsLogger.Log("... truncated ...")
			r.printed++
			return err
		}

		// Create a single log line with all columns
		var values = db.DumpRecursive(dest, " ")
		r.readRowsLogger.Log(fmt.Sprintf("Row: %s", values))
		r.printed++
	}

	return err
}

// Close closes the rows iterator
func (r *wrappedRows) Close() error {
	return r.rows.Close()
}

// accountTime adds elapsed time since the given time to the atomic counter
func accountTime(t *atomic.Int64, since time.Time) {
	t.Add(time.Since(since).Nanoseconds())
}

// wrappedQuerier implements the querier interface with additional functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode support
type wrappedQuerier struct {
	q querier

	prepareTime *atomic.Int64 // Preparation time counter
	execTime    *atomic.Int64 // Execution time counter
	queryTime   *atomic.Int64 // Query time counter
	deallocTime *atomic.Int64 // Deallocation time counter

	dryRun      bool
	queryLogger db.Logger
}

// execContext implements querier.execContext with timing, logging and dry-run support
func (wq wrappedQuerier) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer accountTime(wq.execTime, time.Now())

	if wq.queryLogger != nil {
		if wq.dryRun {
			if !strings.Contains(query, "\n") {
				wq.queryLogger.Log(fmt.Sprintf("-- %s -- skip because of 'dry-run' mode", query))
			} else {
				wq.queryLogger.Log("-- skip because of 'dry-run' mode")
				formattedQuery := fmt.Sprintf("/*\n%s\n*/", query)
				wq.queryLogger.Log(formattedQuery)
			}
		} else {
			wq.queryLogger.Log(query)
		}
	}

	if wq.dryRun {
		return &sqlSurrogateResult{}, nil
	}

	return wq.q.execContext(ctx, query, args...)
}

// queryRowContext implements querier.queryRowContext with timing and logging
func (wq wrappedQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer accountTime(wq.queryTime, time.Now())

	if wq.queryLogger != nil {
		wq.queryLogger.Log(query)
	}

	return wq.q.queryRowContext(ctx, query, args...)
}

// queryContext implements querier.queryContext with timing and logging
func (wq wrappedQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer accountTime(wq.queryTime, time.Now())

	if wq.queryLogger != nil {
		wq.queryLogger.Log(query, args...)
	}

	return wq.q.queryContext(ctx, query, args...)
}

// prepareContext implements querier.prepareContext with timing and logging
func (wq wrappedQuerier) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	defer accountTime(wq.prepareTime, time.Now())

	if wq.queryLogger != nil {
		wq.queryLogger.Log(fmt.Sprintf("PREPARE stmt FROM '%s';", query))
	}

	var stmt, err = wq.q.prepareContext(ctx, query)
	if err != nil {
		return stmt, err
	}

	return &wrappedStatement{
		stmt:        stmt,
		execTime:    wq.execTime,
		deallocTime: wq.deallocTime,
		dryRun:      wq.dryRun,
		queryLogger: wq.queryLogger,
	}, nil
}

// wrappedStatement implements sqlStatement interface with additional functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode support
type wrappedStatement struct {
	stmt sqlStatement

	execTime    *atomic.Int64 // Execution time counter
	deallocTime *atomic.Int64 // Deallocation time counter

	dryRun      bool
	queryLogger db.Logger
}

// Exec executes a prepared statement with timing, logging and dry-run support
func (ws *wrappedStatement) Exec(args ...any) (db.Result, error) {
	defer accountTime(ws.execTime, time.Now())

	if ws.queryLogger != nil {
		if ws.dryRun {
			ws.queryLogger.Log("-- EXECUTE stmt -- skip because of 'dry-run' mode")
		} else {
			ws.queryLogger.Log("EXECUTE stmt;")
		}
	}

	if ws.dryRun {
		return &sqlSurrogateResult{}, nil
	}

	return ws.stmt.Exec(args...)
}

// Close closes the prepared statement with timing and logging
func (ws *wrappedStatement) Close() error {
	defer accountTime(ws.deallocTime, time.Now())

	if ws.queryLogger != nil {
		ws.queryLogger.Log("DEALLOCATE PREPARE stmt;")
	}

	return ws.stmt.Close()
}

// wrappedTransaction implements the transaction interface with additional functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode support
type wrappedTransaction struct {
	tx transaction

	prepareTime *atomic.Int64 // Preparation time counter
	execTime    *atomic.Int64 // Execution time counter
	queryTime   *atomic.Int64 // Query time counter
	deallocTime *atomic.Int64 // Deallocation time counter
	commitTime  *atomic.Int64 // Commit time counter

	dryRun         bool
	queryLogger    db.Logger
	txNotSupported bool
}

// execContext implements querier.execContext within a transaction
func (wtx wrappedTransaction) execContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	defer accountTime(wtx.execTime, time.Now())

	if wtx.queryLogger != nil {
		if wtx.dryRun {
			if !strings.Contains(query, "\n") {
				wtx.queryLogger.Log(fmt.Sprintf("-- %s -- skip because of 'dry-run' mode", query))
			} else {
				wtx.queryLogger.Log("-- skip because of 'dry-run' mode")
				formattedQuery := fmt.Sprintf("/*\n%s\n*/", query)
				wtx.queryLogger.Log(formattedQuery)
			}
		} else {
			wtx.queryLogger.Log(query)
		}
	}

	if wtx.dryRun {
		return &sqlSurrogateResult{}, nil
	}

	return wtx.tx.execContext(ctx, query, args...)
}

// queryRowContext implements querier.queryRowContext within a transaction
func (wtx wrappedTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer accountTime(wtx.queryTime, time.Now())

	if wtx.queryLogger != nil {
		wtx.queryLogger.Log(query)
	}

	return wtx.tx.queryRowContext(ctx, query, args...)
}

// queryContext implements querier.queryContext within a transaction
func (wtx wrappedTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer accountTime(wtx.queryTime, time.Now())

	if wtx.queryLogger != nil {
		wtx.queryLogger.Log(query)
	}

	return wtx.tx.queryContext(ctx, query, args...)
}

// prepareContext implements querier.prepareContext within a transaction
func (wtx wrappedTransaction) prepareContext(ctx context.Context, query string) (sqlStatement, error) {
	defer accountTime(wtx.prepareTime, time.Now())

	if wtx.queryLogger != nil {
		wtx.queryLogger.Log(fmt.Sprintf("PREPARE stmt FROM '%s';", query))
	}

	var stmt, err = wtx.tx.prepareContext(ctx, query)
	if err != nil {
		return stmt, err
	}

	return &wrappedStatement{
		stmt:        stmt,
		execTime:    wtx.execTime,
		deallocTime: wtx.deallocTime,
		dryRun:      wtx.dryRun,
		queryLogger: wtx.queryLogger,
	}, nil
}

// commit implements transaction.commit with timing and logging
func (wtx wrappedTransaction) commit() error {
	defer accountTime(wtx.commitTime, time.Now())

	if wtx.queryLogger != nil {
		if wtx.txNotSupported {
			wtx.queryLogger.Log("-- COMMIT -- skip because dialect does not support transactions")
		} else {
			wtx.queryLogger.Log("COMMIT")
		}
	}

	return wtx.tx.commit()
}

// rollback implements transaction.rollback with timing and logging
func (wtx wrappedTransaction) rollback() error {
	defer accountTime(wtx.commitTime, time.Now())

	if wtx.queryLogger != nil {
		if wtx.txNotSupported {
			wtx.queryLogger.Log("-- ROLLBACK -- skip because dialect does not support transactions")
		} else {
			wtx.queryLogger.Log("ROLLBACK")
		}
	}

	return wtx.tx.rollback()
}

// wrappedTransactor implements the transactor interface with additional functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode support
type wrappedTransactor struct {
	t transactor

	beginTime   *atomic.Int64 // Transaction begin time counter
	prepareTime *atomic.Int64 // Preparation time counter
	execTime    *atomic.Int64 // Execution time counter
	queryTime   *atomic.Int64 // Query time counter
	deallocTime *atomic.Int64 // Deallocation time counter
	commitTime  *atomic.Int64 // Commit time counter

	dryRun bool

	queryLogger db.Logger

	txNotSupported bool
}

// begin implements transactor.begin with timing and logging
func (wt wrappedTransactor) begin(ctx context.Context) (transaction, error) {
	defer accountTime(wt.beginTime, time.Now())

	if wt.queryLogger != nil {
		if wt.txNotSupported {
			wt.queryLogger.Log("-- BEGIN -- skip because dialect does not support transactions")
		} else {
			wt.queryLogger.Log("BEGIN")
		}
	}

	var t, err = wt.t.begin(ctx)

	if err != nil {
		return t, err
	}

	return wrappedTransaction{
		tx:             t,
		prepareTime:    wt.prepareTime,
		execTime:       wt.execTime,
		queryTime:      wt.queryTime,
		deallocTime:    wt.deallocTime,
		commitTime:     wt.commitTime,
		dryRun:         wt.dryRun,
		queryLogger:    wt.queryLogger,
		txNotSupported: wt.txNotSupported,
	}, nil
}
