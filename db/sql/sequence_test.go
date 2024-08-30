package sql

import "github.com/acronis/perfkit/db"

func (suite *TestingSuite) TestSequence() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	if d.DialectName() == db.CLICKHOUSE || d.DialectName() == db.CASSANDRA {
		return
	}

	if err := d.CreateSequence("perf_table_seq"); err != nil {
		suite.T().Error(err)
		return
	}

	defer func() {
		if err := d.DropSequence("perf_table_seq"); err != nil {
			suite.T().Error(err)
		}
	}()

	var nextVal, err = s.GetNextVal("perf_table_seq")
	if err != nil {
		suite.T().Error(err)
		return
	}

	var prevVal = nextVal

	if nextVal, err = s.GetNextVal("perf_table_seq"); err != nil {
		suite.T().Error(err)
		return
	}

	if nextVal != prevVal+1 {
		suite.T().Errorf("expected %d, got %d", prevVal+1, nextVal)
		return
	}
}
