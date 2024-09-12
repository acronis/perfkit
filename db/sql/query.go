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

func (g *sqlGateway) Exec(format string, args ...interface{}) (db.Result, error) {
	var sqlRes, err = g.rw.execContext(g.ctx, format, args...)
	return &sqlResult{result: sqlRes}, err
}

func (g *sqlGateway) QueryRow(format string, args ...interface{}) db.Row {
	var row = g.rw.queryRowContext(g.ctx, format, args...)
	return row
}

func (g *sqlGateway) Query(format string, args ...interface{}) (db.Rows, error) {
	var rows, err = g.rw.queryContext(g.ctx, format, args...)
	return &sqlRows{rows: rows}, err
}
