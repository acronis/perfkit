package sql

import (
	"context"
	"database/sql"
	"fmt"
	"go.uber.org/atomic"
	"strings"
	"time"

	"github.com/acronis/perfkit/db"
)

const maxRowsToPrint = 10

// wrappedRow is a struct for storing DB *sql.Row
type wrappedRow struct {
	row *sql.Row

	readRowsLogger db.Logger
}

func (r *wrappedRow) Scan(dest ...any) error {
	var err = r.row.Scan(dest...)

	if r.readRowsLogger != nil {
		// Create a single log line with all columns
		var values = db.DumpRecursive(dest, " ")
		r.readRowsLogger.Log(fmt.Sprintf("Row: %s", values))
	}

	return err
}

// wrappedRows is a struct for storing DB *sql.Rows (as a slice of Row) and current index
type wrappedRows struct {
	rows *sql.Rows

	readRowsLogger db.Logger
	printed        int
}

func (r *wrappedRows) Next() bool {
	return r.rows.Next()
}

func (r *wrappedRows) Err() error {
	return r.rows.Err()
}

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

func (r *wrappedRows) Close() error {
	return r.rows.Close()
}

func accountTime(t *atomic.Int64, since time.Time) {
	t.Add(time.Since(since).Nanoseconds())
}

// wrappedQuerier is a wrapper for querier that implements following functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode
type wrappedQuerier struct {
	q querier

	prepareTime *atomic.Int64 // *time.Duration
	execTime    *atomic.Int64 // *time.Duration
	queryTime   *atomic.Int64 // *time.Duration
	deallocTime *atomic.Int64 // *time.Duration

	dryRun      bool
	queryLogger db.Logger
}

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

func (wq wrappedQuerier) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer accountTime(wq.queryTime, time.Now())

	if wq.queryLogger != nil {
		wq.queryLogger.Log(query)
	}

	return wq.q.queryRowContext(ctx, query, args...)
}

func (wq wrappedQuerier) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer accountTime(wq.queryTime, time.Now())

	if wq.queryLogger != nil {
		wq.queryLogger.Log(query, args...)
	}

	return wq.q.queryContext(ctx, query, args...)
}

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

// wrappedStatement is a wrapper for sqlStmt that adds additional features:
// - measuring time of queries
// - logging of queries
// - dry-run mode
type wrappedStatement struct {
	stmt sqlStatement

	execTime    *atomic.Int64 // *time.Duration
	deallocTime *atomic.Int64 // *time.Duration

	dryRun      bool
	queryLogger db.Logger
}

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

func (ws *wrappedStatement) Close() error {
	defer accountTime(ws.deallocTime, time.Now())

	if ws.queryLogger != nil {
		ws.queryLogger.Log("DEALLOCATE PREPARE stmt;")
	}

	return ws.stmt.Close()
}

// wrappedTransaction is a wrapper for transaction that implements following functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode
type wrappedTransaction struct {
	tx transaction

	prepareTime *atomic.Int64 // *time.Duration
	execTime    *atomic.Int64 // *time.Duration
	queryTime   *atomic.Int64 // *time.Duration
	deallocTime *atomic.Int64 // *time.Duration
	commitTime  *atomic.Int64 // *time.Duration

	dryRun         bool
	queryLogger    db.Logger
	txNotSupported bool
}

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

func (wtx wrappedTransaction) queryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	defer accountTime(wtx.queryTime, time.Now())

	if wtx.queryLogger != nil {
		wtx.queryLogger.Log(query)
	}

	return wtx.tx.queryRowContext(ctx, query, args...)
}

func (wtx wrappedTransaction) queryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	defer accountTime(wtx.queryTime, time.Now())

	if wtx.queryLogger != nil {
		wtx.queryLogger.Log(query)
	}

	return wtx.tx.queryContext(ctx, query, args...)
}

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

// wrappedTransactor is a wrapper for transactor that implements following functionality:
// - measuring time of queries
// - logging of queries
// - dry-run mode
type wrappedTransactor struct {
	t transactor

	beginTime   *atomic.Int64 // *time.Duration
	prepareTime *atomic.Int64 // *time.Duration
	execTime    *atomic.Int64 // *time.Duration
	queryTime   *atomic.Int64 // *time.Duration
	deallocTime *atomic.Int64 // *time.Duration
	commitTime  *atomic.Int64 // *time.Duration

	dryRun bool

	queryLogger db.Logger

	txNotSupported bool
}

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
