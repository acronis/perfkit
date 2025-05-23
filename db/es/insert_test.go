package es

import (
	"time"

	"github.com/acronis/perfkit/db"
)

func (suite *TestingSuite) TestInsert() {
	d, s, c := suite.makeTestSession()
	defer logDbTime(suite.T(), c)
	defer cleanup(suite.T(), d)

	type testStruct struct {
		Timestamp    time.Time `db:"@timestamp"`
		Id           int64     `db:"id"`
		Uuid         string    `db:"uuid"`
		TestType     string    `db:"type"`
		PolicyName   string    `db:"policy_name"`
		ResourceName string    `db:"resource_name"`
		Accessors    []string  `db:"accessors"`
		StartTime    time.Time `db:"start_time"`
	}

	var now = time.Now().UTC()

	var testStructs = []testStruct{
		{
			now,
			1,
			"00000000-0000-0000-0000-000000000000",
			"test",
			"policy",
			"resource",
			[]string{"tenant_1", "tenant_2"},
			now,
		},
		{
			now.Add(1 * time.Second),
			2,
			"00000000-0000-0000-0000-000000000002",
			"test_type",
			"secret_policy",
			"new_resource",
			[]string{"tenant_2"},
			now.Add(1 * time.Second),
		},
	}

	var toInsert [][]interface{}
	for _, ts := range testStructs {
		toInsert = append(toInsert, []interface{}{
			ts.Timestamp,
			ts.Id,
			ts.Uuid,
			ts.TestType,
			ts.PolicyName,
			ts.ResourceName,
			ts.Accessors,
			ts.StartTime,
		})
	}

	if err := s.BulkInsert("perf_table", toInsert, []string{"@timestamp", "id", "uuid", "type", "policy_name", "resource_name", "accessors", "start_time"}); err != nil {
		suite.T().Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	if rows, err := s.Select("perf_table", &db.SelectCtrl{
		Fields: []string{"id", "uuid", "type", "policy_name", "accessors", "start_time"},
		Where: map[string][]string{
			"accessors": {"tenant_2"},
		},
		Order: []string{"desc(start_time)"},
	}); err != nil {
		suite.T().Error(err)
		return
	} else {
		defer rows.Close()
		suite.T().Log("rows", rows)

		for rows.Next() {
			var id int64
			var uuid, testType, policyName string
			var accessors []string
			var startTime time.Time

			if scanErr := rows.Scan(&id, &uuid, &testType, &policyName, &accessors, &startTime); scanErr != nil {
				suite.T().Error(scanErr)
				return
			}
			suite.T().Log("row", id, uuid, testType, policyName, accessors, startTime)
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

	rows.Close()
}
