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
	if g.explain {
		var err error
		if format, err = addExplainPrefix(g.dialect.name(), format); err != nil {
			return &db.EmptyRow{}
		}

		var rows *sql.Rows
		if rows, err = g.rw.queryContext(g.ctx, format, args...); err != nil {
			return &db.EmptyRow{}
		}

		if g.explainLogger != nil {
			if err = logExplainResults(g.explainLogger, g.dialect.name(), rows, format, args...); err != nil {
				return &db.EmptyRow{}
			}
		}
	}

	var row = g.rw.queryRowContext(g.ctx, format, args...)
	return &sqlRow{row: row, readRowsLogger: g.readRowsLogger}
}

func (g *sqlGateway) Query(format string, args ...interface{}) (db.Rows, error) {
	if g.explain {
		var err error
		if format, err = addExplainPrefix(g.dialect.name(), format); err != nil {
			return &db.EmptyRows{}, err
		}
	}

	var rows, err = g.rw.queryContext(g.ctx, format, args...)
	if err != nil {
		return rows, err
	}

	if g.explain && g.explainLogger != nil {
		if err = logExplainResults(g.explainLogger, g.dialect.name(), rows, format, args...); err != nil {
			return &db.EmptyRows{}, err
		}
	}

	return &sqlRows{rows: rows, readRowsLogger: g.readRowsLogger}, err
}
