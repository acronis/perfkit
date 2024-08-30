package sql

import "github.com/acronis/perfkit/db"

func (suite *TestingSuite) TestQuery() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	var qry string
	switch d.DialectName() {
	case db.CASSANDRA:
		qry = "SELECT cluster_name FROM system.local;"
	default:
		qry = "SELECT 1;"
	}

	var row = s.QueryRow(qry)
	switch d.DialectName() {
	case db.CASSANDRA:
		var resp string
		if err := row.Scan(&resp); err != nil {
			suite.T().Error(err)
			return
		}
	default:
		var resp int
		if err := row.Scan(&resp); err != nil {
			suite.T().Error(err)
			return
		}

		if resp != 1 {
			suite.T().Error("unexpected response", resp)
		}
	}

	switch d.DialectName() {
	case db.CASSANDRA:
		qry = `
			BEGIN BATCH
				INSERT INTO perf_table (origin, type, name) VALUES (2, 2, 'test');
				INSERT INTO perf_table (origin, type, name) VALUES (3, 4, 'perf');
			APPLY BATCH;
		`
	default:
		qry = `
			INSERT INTO perf_table (origin, type, name)
			VALUES (2, 2, 'test'),
			       (3, 4, 'perf');
		`
	}

	if result, err := s.Exec(qry); err != nil {
		suite.T().Error(err)
		return
	} else {
		suite.T().Log("inserted")

		if d.DialectName() != db.MSSQL &&
			d.DialectName() != db.POSTGRES &&
			d.DialectName() != db.CLICKHOUSE &&
			d.DialectName() != db.CASSANDRA {
			if id, insertErr := result.LastInsertId(); insertErr != nil {
				suite.T().Error(insertErr)
			} else if id != 3 {
				suite.T().Log("last inserted id", id)
			}
		}

		if d.DialectName() != db.CASSANDRA {
			if rowsAffected, rowsErr := result.RowsAffected(); rowsErr != nil {
				suite.T().Error(rowsErr)
			} else if rowsAffected != 2 {
				if d.DialectName() != db.CLICKHOUSE {
					suite.T().Error("rows affected", rowsAffected)
				}
			}
		}
	}

	if rows, err := s.Query("SELECT * FROM perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else {
		defer rows.Close()
		suite.T().Log("rows", rows)

		for rows.Next() {
			var origin, testType int
			var name string
			if scanErr := rows.Scan(&origin, &testType, &name); scanErr != nil {
				suite.T().Error(scanErr)
				return
			}
			suite.T().Log("row", origin, testType, name)
		}
	}

	if err := s.Transact(func(tx db.DatabaseAccessor) error {
		_, err := tx.Exec("INSERT INTO perf_table (origin, type, name) VALUES (5, 6, 'next');")
		return err
	}); err != nil {
		suite.T().Error(err)
	}

	if rows, err := s.Query("SELECT * FROM perf_table"); err != nil {
		suite.T().Error(err)
		return
	} else {
		defer rows.Close()
		suite.T().Log("rows", rows)

		for rows.Next() {
			var origin, testType int
			var name string
			if scanErr := rows.Scan(&origin, &testType, &name); scanErr != nil {
				suite.T().Error(scanErr)
				return
			}
			suite.T().Log("row", origin, testType, name)
		}
	}
}
