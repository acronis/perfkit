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

	if err := s.InsertInto("perf_table", testStructs, []string{"@timestamp", "id", "uuid", "type", "policy_name", "resource_name", "accessors", "start_time"}); err != nil {
		suite.T().Error(err)
		return
	}

	time.Sleep(2 * time.Second)

	if rows, err := s.Search("perf_table", &db.SelectCtrl{
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
