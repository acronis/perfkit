package sql

import (
	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) TestPrepareStatement() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	var qry string
	switch d.DialectName() {
	case db.MSSQL, db.POSTGRES:
		qry = "INSERT INTO perf_table (origin, type, name) VALUES ($1, $2, $3);"
	default:
		qry = "INSERT INTO perf_table (origin, type, name) VALUES (?, ?, ?);"
	}

	var args = []interface{}{}
	switch d.DialectName() {
	default:
		args = []interface{}{2, 2, "test"}
	}

	if txErr := s.Transact(func(tx db.DatabaseAccessor) error {
		var stmt, err = tx.Prepare(qry)
		if err != nil {
			return err
		}

		defer stmt.Close()

		if rows, stmtErr := stmt.Exec(args...); stmtErr != nil {
			return stmtErr
		} else {
			if d.DialectName() != db.CLICKHOUSE {
				if rowsAffected, rowsErr := rows.RowsAffected(); rowsErr != nil {
					return err
				} else if rowsAffected != 1 {
					return err
				}
			}
		}

		return nil
	}); txErr != nil {
		suite.T().Error(txErr)
	}
}
