package sql

func (suite *TestingSuite) TestInsert() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	type testStruct struct {
		Origin   int    `db:"origin"`
		TestType int    `db:"type"`
		Name     string `db:"name"`
	}

	var test = testStruct{2, 2, "test"}

	if err := s.InsertInto("perf_table", test, []string{"origin", "type", "name"}); err != nil {
		suite.T().Error(err)
		return
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
