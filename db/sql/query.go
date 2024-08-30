package sql

import (
	"database/sql"

	"github.com/acronis/perfkit/db"
)

type sqlResult struct {
	result sql.Result
}

func (r *sqlResult) LastInsertId() (int64, error) {
	return r.result.LastInsertId()
}

func (r *sqlResult) RowsAffected() (int64, error) {
	return r.result.RowsAffected()
}

func (a *gateway) Exec(format string, args ...interface{}) (db.Result, error) {
	var sqlRes, err = a.rw.ExecContext(a.ctx, format, args...)
	return &sqlResult{result: sqlRes}, err
}

func (a *gateway) QueryRow(format string, args ...interface{}) db.Row {
	var row = a.rw.QueryRowContext(a.ctx, format, args...)
	return row
}

func (a *gateway) Query(format string, args ...interface{}) (db.Rows, error) {
	var rows, err = a.rw.QueryContext(a.ctx, format, args...)
	return &sqlRows{rows: rows}, err
}

func (a *gateway) ExecWithPlaceholders(query string) error {
	return applyMigrations(a.rw, a.dialect, "", query)
}
