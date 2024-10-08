package sql

import "github.com/acronis/perfkit/db"

func (suite *TestingSuite) TestInsert() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	if err := s.BulkInsert("perf_table", [][]interface{}{
		{2, 2, "test"},
	}, []string{"origin", "type", "name"}); err != nil {
		suite.T().Error(err)
		return
	}

	if rows, err := s.Select("perf_table",
		&db.SelectCtrl{
			Fields: []string{"origin", "type"},
			Where: map[string][]string{
				"name": {"test"},
			},
		}); err != nil {
		suite.T().Error(err)
		return
	} else {
		defer rows.Close()
		suite.T().Log("rows", rows)

		for rows.Next() {
			var origin, testType int
			if scanErr := rows.Scan(&origin, &testType); scanErr != nil {
				suite.T().Error(scanErr)
				return
			}
			suite.T().Log("row", origin, testType)
		}
	}

}

func (suite *TestingSuite) TestSelect() {
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

	var selectCtrl = &db.SelectCtrl{
		Fields: []string{"origin"},
	}
	selectCtrl.Page.Limit = 2

	switch d.DialectName() {
	case db.CASSANDRA:
		selectCtrl.Where = map[string][]string{
			"origin": {"3"},
			"type":   {"4"},
			"name":   {"perf"},
		}
	default:
		selectCtrl.Where = map[string][]string{
			"name": {"perf"},
		}
	}

	if rows, err := s.Select("perf_table", selectCtrl); err != nil {
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

	if rows, err := s.Select("perf_table", &db.SelectCtrl{Fields: []string{"COUNT(0)"}}); err != nil {
		suite.T().Error(err)
		return
	} else if rowsErr := rows.Err(); rowsErr != nil {
		suite.T().Error(rowsErr)
	} else {
		defer rows.Close()

		var values []int64
		for rows.Next() {
			var rowNum int64
			if scanErr := rows.Scan(&rowNum); scanErr != nil {
				suite.T().Error(scanErr)
				return
			}
			values = append(values, rowNum)
		}

		if len(values) != 1 {
			suite.T().Error("unexpected number of rows", len(values))
			return
		}

		if values[0] != 2 {
			suite.T().Error("unexpected value", values[0])
			return
		}
	}
}
