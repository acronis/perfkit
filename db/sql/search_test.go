package sql

import "github.com/acronis/perfkit/db"

func (suite *TestingSuite) TestSearch() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	var qry string
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

	if _, err := s.Exec(qry); err != nil {
		suite.T().Error(err)
		return
	}

	var searchCondition string
	switch d.DialectName() {
	case db.CASSANDRA:
		searchCondition = "origin = 3 AND type = 4 AND name = 'perf'"
	default:
		searchCondition = "name = 'perf'"
	}

	if rows, err := s.Search("perf_table", "origin", searchCondition, "", 2, false); err != nil {
		suite.T().Error(err)
		return
	} else if rowsErr := rows.Err(); rowsErr != nil {
		suite.T().Error(rowsErr)
	} else {
		defer rows.Close()

		var values []int
		for rows.Next() {
			var origin int
			if scanErr := rows.Scan(&origin); scanErr != nil {
				suite.T().Error(scanErr)
				return
			}
			values = append(values, origin)
		}

		if len(values) != 1 {
			suite.T().Error("unexpected number of rows", len(values))
			return
		}

		if values[0] != 3 {
			suite.T().Error("unexpected value", values[0])
			return
		}
	}
}
