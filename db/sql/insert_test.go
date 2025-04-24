package sql

import (
	"github.com/acronis/perfkit/db"
)

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
		Fields: []string{"origin", "name"},
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
			var name string
			if scanErr := rows.Scan(&origin, &name); scanErr != nil {
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

// TestSelectOne tests the basic database connectivity by executing a "SELECT 1" query.
// This is a common pattern used to verify that a database connection is alive
// without relying on any specific tables or data structures.
func (suite *TestingSuite) TestSelectOne() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	// Create a simple SelectCtrl that requests the literal value "1"
	selectCtrl := &db.SelectCtrl{
		Fields: []string{"1"},
	}

	// Execute the "SELECT 1" query
	rows, err := s.Select("", selectCtrl)
	if err != nil {
		suite.T().Errorf("Failed to execute SELECT 1: %v", err)
		return
	}
	defer rows.Close()

	if d.DialectName() == db.CASSANDRA {
		// Cassandra driver returns empty rows on SELECT 1
		return
	}

	// Check if there's at least one row returned
	if !rows.Next() {
		suite.T().Error("No rows returned from SELECT 1 query")
		return
	}

	// Scan the result
	var result int
	if err := rows.Scan(&result); err != nil {
		suite.T().Errorf("Failed to scan result: %v", err)
		return
	}

	// Verify the result is 1
	if result != 1 {
		suite.T().Errorf("Expected result to be 1, got %d", result)
		return
	}

	// Check there are no more rows (should be only one)
	if rows.Next() {
		suite.T().Error("More than one row returned from SELECT 1 query")
		return
	}

	// Check for any errors during iteration
	if err := rows.Err(); err != nil {
		suite.T().Errorf("Error during row iteration: %v", err)
		return
	}
}
