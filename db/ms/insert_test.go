package ms

import (
	"time"

	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) TestInsert() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	var toInsert [][]interface{}
	toInsert = append(toInsert, []interface{}{
		int64(1),
		"00000000-0000-0000-0000-000000000001",
		"test",
		"policy_a",
		"resource_a",
		int64(100),
	})
	toInsert = append(toInsert, []interface{}{
		int64(2),
		"00000000-0000-0000-0000-000000000002",
		"test_type",
		"policy_b",
		"resource_b",
		int64(200),
	})

	columnNames := []string{"id", "uuid", "type", "policy_name", "resource_name", "score"}

	if err := s.BulkInsert("ms_perf_table", toInsert, columnNames); err != nil {
		suite.T().Error(err)
		return
	}

	// Meilisearch indexes asynchronously; wait for indexing
	time.Sleep(2 * time.Second)

	rows, err := s.Select("ms_perf_table", &db.SelectCtrl{
		Fields: []string{"id", "uuid", "type", "policy_name", "score"},
		Order:  []string{"desc(score)"},
	})
	if err != nil {
		suite.T().Error(err)
		return
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var id, score int64
		var uuid, testType, policyName string

		if scanErr := rows.Scan(&id, &uuid, &testType, &policyName, &score); scanErr != nil {
			suite.T().Error(scanErr)
			return
		}
		suite.T().Log("row", id, uuid, testType, policyName, score)
		count++
	}

	suite.Equal(2, count, "expected 2 rows")
}

func (suite *TestingSuite) TestSelectWithFilter() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	var toInsert [][]interface{}
	toInsert = append(toInsert, []interface{}{int64(10), "uuid-a", "typeA", "policy_x", "res_x", int64(50)})
	toInsert = append(toInsert, []interface{}{int64(20), "uuid-b", "typeB", "policy_y", "res_y", int64(150)})
	toInsert = append(toInsert, []interface{}{int64(30), "uuid-c", "typeA", "policy_z", "res_z", int64(250)})

	columnNames := []string{"id", "uuid", "type", "policy_name", "resource_name", "score"}

	if err := s.BulkInsert("ms_perf_table", toInsert, columnNames); err != nil {
		suite.T().Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	rows, err := s.Select("ms_perf_table", &db.SelectCtrl{
		Fields: []string{"id", "type", "score"},
		Where: map[string][]string{
			"type": {"typeA"},
		},
		Order: []string{"asc(id)"},
	})
	if err != nil {
		suite.T().Error(err)
		return
	}
	defer rows.Close()

	var count int
	for rows.Next() {
		var id, score int64
		var tp string

		if scanErr := rows.Scan(&id, &tp, &score); scanErr != nil {
			suite.T().Error(scanErr)
			return
		}
		suite.T().Log("row", id, tp, score)
		suite.Equal("typeA", tp)
		count++
	}

	suite.Equal(2, count, "expected 2 rows matching type=typeA")
}

func (suite *TestingSuite) TestSelectCount() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	var toInsert [][]interface{}
	toInsert = append(toInsert, []interface{}{int64(1), "u1", "t1", "p1", "r1", int64(10)})
	toInsert = append(toInsert, []interface{}{int64(2), "u2", "t2", "p2", "r2", int64(20)})
	toInsert = append(toInsert, []interface{}{int64(3), "u3", "t1", "p3", "r3", int64(30)})

	columnNames := []string{"id", "uuid", "type", "policy_name", "resource_name", "score"}

	if err := s.BulkInsert("ms_perf_table", toInsert, columnNames); err != nil {
		suite.T().Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	rows, err := s.Select("ms_perf_table", &db.SelectCtrl{
		Fields: []string{"COUNT(0)"},
	})
	if err != nil {
		suite.T().Error(err)
		return
	}
	defer rows.Close()

	suite.True(rows.Next())

	var count int64
	if scanErr := rows.Scan(&count); scanErr != nil {
		suite.T().Error(scanErr)
		return
	}

	suite.Equal(int64(3), count, "expected count of 3")
}
