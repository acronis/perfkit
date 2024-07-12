package sql

import (
	"database/sql"
	"strings"
)

// Rows is a struct for storing DB rows (as a slice of Row) and current index
type sqlRows struct {
	rows *sql.Rows
}

func (r *sqlRows) Next() bool {
	return r.rows.Next()
}

func (r *sqlRows) Err() error {
	return r.rows.Err()
}

func (r *sqlRows) Scan(dest ...interface{}) error {
	return r.rows.Scan(dest...)
}

func (r *sqlRows) Close() error {
	return r.rows.Close()
}

func (r *sqlRows) Dump() string {
	var size = 10

	ret := make([]string, 0, size)

	/*
		for n, row := range r.Data {
			if n > size {
				// do not flood the logs
				ret = append(ret, "... truncated ...")

				break
			}
			ret = append(ret, DumpRecursive(row, " "))
		}

	*/

	return "[" + strings.Join(ret, ", ") + "]"
}
