package sql

import (
	"database/sql"
	"fmt"

	"github.com/acronis/perfkit/db"
)

const maxRowsToPrint = 10

// sqlRow is a struct for storing DB *sql.Row
type sqlRow struct {
	row *sql.Row

	readRowsLogger db.Logger
}

func (r *sqlRow) Scan(dest ...any) error {
	var err = r.row.Scan(dest...)

	if r.readRowsLogger != nil {
		// Create a single log line with all columns
		var values = db.DumpRecursive(dest, " ")
		r.readRowsLogger.Log(fmt.Sprintf("Row: %s", values))
	}

	return err
}

// sqlRows is a struct for storing DB *sql.Rows (as a slice of Row) and current index
type sqlRows struct {
	rows *sql.Rows

	readRowsLogger db.Logger
	printed        int
}

func (r *sqlRows) Next() bool {
	return r.rows.Next()
}

func (r *sqlRows) Err() error {
	return r.rows.Err()
}

func (r *sqlRows) Scan(dest ...interface{}) error {
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

func (r *sqlRows) Close() error {
	return r.rows.Close()
}
