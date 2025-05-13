package sql

import (
	"github.com/acronis/perfkit/db"
	"github.com/stretchr/testify/assert"
)

func (suite *TestingSuite) TestUpdate() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	// Skip update tests for ClickHouse and Cassandra since they don't support updating primary key columns
	if d.DialectName() == db.CLICKHOUSE || d.DialectName() == db.CASSANDRA {
		suite.T().Skip("ClickHouse and Cassandra don't support updating primary key columns")
		return
	}

	// Insert test data
	if err := s.BulkInsert("perf_table", [][]interface{}{
		{1, 1, "test1"},
		{2, 2, "test2"},
		{3, 3, "test3"},
	}, []string{"origin", "type", "name"}); err != nil {
		suite.T().Error(err)
		return
	}

	// Test 1: Basic update - update a single field
	var affected int64
	err := s.Transact(func(tx db.DatabaseAccessor) error {
		var err error
		affected, err = tx.Update("perf_table",
			&db.UpdateCtrl{
				Set: map[string]interface{}{
					"type": "3",
				},
				Where: map[string][]string{
					"name": {"test1"},
				},
			})
		return err
	})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(1), affected)

	// Verify the update
	rows, err := s.Select("perf_table",
		&db.SelectCtrl{
			Fields: []string{"origin", "type", "name"},
			Where: map[string][]string{
				"name": {"test1"},
			},
		})
	assert.NoError(suite.T(), err)

	assert.True(suite.T(), rows.Next())
	var origin, testType int
	var name string
	err = rows.Scan(&origin, &testType, &name)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 1, origin)
	assert.Equal(suite.T(), 3, testType)
	assert.Equal(suite.T(), "test1", name)

	rows.Close()

	// Test 2: Update multiple fields
	err = s.Transact(func(tx db.DatabaseAccessor) error {
		var err error
		affected, err = tx.Update("perf_table",
			&db.UpdateCtrl{
				Set: map[string]interface{}{
					"type": "5",
				},
				Where: map[string][]string{
					"name": {"test2"},
				},
			})
		return err
	})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(1), affected)

	// Verify multiple field update
	rows, err = s.Select("perf_table",
		&db.SelectCtrl{
			Fields: []string{"origin", "type", "name"},
			Where: map[string][]string{
				"name": {"test2"},
			},
		})
	assert.NoError(suite.T(), err)

	assert.True(suite.T(), rows.Next())
	err = rows.Scan(&origin, &testType, &name)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 2, origin)
	assert.Equal(suite.T(), 5, testType)
	assert.Equal(suite.T(), "test2", name)

	rows.Close()

	// Test 3: Update with multiple conditions
	err = s.Transact(func(tx db.DatabaseAccessor) error {
		var err error
		affected, err = tx.Update("perf_table",
			&db.UpdateCtrl{
				Set: map[string]interface{}{
					"type": "6",
				},
				Where: map[string][]string{
					"type": {"3"},
					"name": {"test3"},
				},
			})
		return err
	})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(1), affected)

	// Verify update with multiple conditions
	rows, err = s.Select("perf_table",
		&db.SelectCtrl{
			Fields: []string{"origin", "type", "name"},
			Where: map[string][]string{
				"name": {"test3"},
			},
		})
	assert.NoError(suite.T(), err)

	assert.True(suite.T(), rows.Next())
	err = rows.Scan(&origin, &testType, &name)
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), 3, origin)
	assert.Equal(suite.T(), 6, testType)
	assert.Equal(suite.T(), "test3", name)

	rows.Close()

	// Test 4: Update non-existent record
	err = s.Transact(func(tx db.DatabaseAccessor) error {
		var err error
		affected, err = tx.Update("perf_table",
			&db.UpdateCtrl{
				Set: map[string]interface{}{
					"type": "7",
				},
				Where: map[string][]string{
					"name": {"non_existent"},
				},
			})
		return err
	})
	assert.NoError(suite.T(), err)
	assert.Equal(suite.T(), int64(0), affected)

	// Test 5: Update with invalid field name
	err = s.Transact(func(tx db.DatabaseAccessor) error {
		var err error
		_, err = tx.Update("perf_table",
			&db.UpdateCtrl{
				Set: map[string]interface{}{
					"invalid_field": "7",
				},
				Where: map[string][]string{
					"name": {"test1"},
				},
			})
		return err
	})
	assert.Error(suite.T(), err)
}
