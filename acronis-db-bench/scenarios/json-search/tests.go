package json_search

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	tests := []*engine.TestDesc{
		// JSON insert tests
		&TestInsertJSON,
		&TestInsertJSONDBR,

		// JSON select tests with indexed values
		&TestSelectJSONByIndexedValue,
		&TestSearchJSONByIndexedValue,

		// JSON select tests with non-indexed values
		&TestSelectJSONByNonIndexedValue,
		&TestSearchJSONByNonIndexedValue,
	}

	tables := map[string]engine.TestTable{
		TestTableJSON.TableName: TestTableJSON,
	}

	scenario := &engine.TestScenario{
		Name:   "json-search",
		Tests:  tests,
		Tables: tables,
	}

	if err := engine.RegisterTestScenario(scenario); err != nil {
		panic(err)
	}
}

// TestTableJSON is table to store JSON data
var TestTableJSON = engine.TestTable{
	TableName: "acronis_db_bench_json",
	Databases: []db.DialectName{db.MYSQL, db.POSTGRES},
	Columns: [][]interface{}{
		{"previous", "bigint", 0},
		{"sequence", "bigint", 0},
		{"time", "time", 0},
		{"json_data", "json", 0},
		{"source", "string", 0, 32},
		{"specversion", "string", 3, 32},
		{"tag", "string", 256, 32},
		{"type", "cti_uuid", 64},
		{"subject", "uuid", 0},
		{"tenantid", "uuid", 0},
		{"clientid", "uuid", 0},
		{"topic", "cti_uuid", 0},
		{"event_id", "uuid", 0},
		{"date", "timestamp", 0},
		{"created_at", "timestamp", 0},
		{"subjecttype", "cti_uuid", 64},
	},
	InsertColumns: []string{}, // all
	UpdateColumns: []string{"time", "json_data", "date"},
	CreateQuery: `create table acronis_db_bench_json(
			id {$bigint_autoinc_pk},
			previous bigint,
			sequence bigint,
			time text,
			json_data {$json_type},
			database64 text,
			source text not null,
			specversion text not null,
			type text not null,
			subject text,
			tenantid {$uuid} not null,
			clientid {$uuid},
			originid {$uuid},
			dataref text,
			traceparent {$uuid},
			topic text not null,
			tag text not null,
			event_id {$uuid} not null,
			date timestamp not null,
			created_at timestamp not null,
			ingesttime timestamp,
			subjecttype text
			) {$engine};
			{$json_index}`,
	CreateQueryPatchFuncs: []engine.CreateQueryPatchFunc{JSONTableCreateQueryPatchFunc},
	Indexes:               [][]string{{"sequence"}, {"created_at"}},
}

func JSONTableCreateQueryPatchFunc(table string, query string, dialect db.DialectName) (string, error) { //nolint:revive
	switch dialect {
	case db.MYSQL:
		query = strings.ReplaceAll(query, "{$json_type}", "json")
		query = strings.ReplaceAll(query, "{$json_index}",
			"ALTER TABLE acronis_db_bench_json ADD COLUMN _data_f0f0 VARCHAR(1024) AS (JSON_EXTRACT(json_data, '$.field0.field0')) STORED;"+
				"ALTER TABLE acronis_db_bench_json ADD COLUMN _data_f0f0f0 VARCHAR(1024) AS (JSON_EXTRACT(json_data, '$.field0.field0.field0')) STORED;"+
				"CREATE INDEX acronis_db_bench_json_idx_data_f0f0 ON acronis_db_bench_json(_data_f0f0);"+
				"CREATE INDEX acronis_db_bench_json_idx_data_f0f0f0 ON acronis_db_bench_json(_data_f0f0f0);")
	case db.POSTGRES:
		query = strings.ReplaceAll(query, "{$json_type}", "jsonb")
		query = strings.ReplaceAll(query, "{$json_index}",
			"CREATE INDEX acronis_db_bench_json_idx_data ON acronis_db_bench_json USING GIN (json_data jsonb_path_ops)")
	default:
		return "", fmt.Errorf("unsupported driver: '%v', supported drivers are: postgres|mysql", dialect)
	}

	return query, nil
}

// TestInsertJSON inserts a row into a table with JSON(b) column
var TestInsertJSON = engine.TestDesc{
	Name:        "insert-json",
	Metric:      "rows/sec",
	Description: "insert a row into a table with JSON(b) column",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.MYSQL, db.POSTGRES},
	Table:       TestTableJSON,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertJSONDBR inserts a row into a table with JSON(b) column using golang DBR driver
var TestInsertJSONDBR = engine.TestDesc{
	Name:        "dbr-insert-json",
	Metric:      "rows/sec",
	Description: "insert a row into a table with JSON(b) column using golang DBR driver",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableJSON,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestSelectJSONByIndexedValue selects a row from the 'json' table by some json condition
var TestSelectJSONByIndexedValue = engine.TestDesc{
	Name:        "select-json-by-indexed-value",
	Metric:      "rows/sec",
	Description: "select a row from the 'json' table by some json condition",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.MYSQL, db.POSTGRES},
	Table:       TestTableJSON,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		where := func(worker *benchmark.BenchmarkWorker) string {
			id := worker.Randomizer.Uintn64(testDesc.Table.RowsCount - 1)

			var dialectName, err = db.GetDialectName(b.TestOpts.(*engine.TestOpts).DBOpts.ConnString)
			if err != nil {
				b.Exit(err)
			}

			switch dialectName {
			case db.MYSQL:
				return "_data_f0f0 = '10' AND id > " + strconv.FormatUint(id, 10)
			case db.POSTGRES:
				return "json_data @> '{\"field0\": {\"field0\": 10}}' AND id > " + strconv.FormatUint(id, 10)
			default:
				b.Exit("The %s test is not supported on driver: %s", testDesc.Name, dialectName)
			}

			return ""
		}
		orderby := func(worker *benchmark.BenchmarkWorker) string { //nolint:revive
			return "id ASC"
		}
		engine.TestSelectRawSQLQuery(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSearchJSONByIndexedValue searches a row from the 'json' table using some json condition using LIKE {}
var TestSearchJSONByIndexedValue = engine.TestDesc{
	Name:        "search-json-by-indexed-value",
	Metric:      "rows/sec",
	Description: "search a row from the 'json' table using some json condition using LIKE {}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.MYSQL, db.POSTGRES},
	Table:       TestTableJSON,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		where := func(worker *benchmark.BenchmarkWorker) string {
			id := worker.Randomizer.Uintn64(testDesc.Table.RowsCount - 1)

			var dialectName, err = db.GetDialectName(b.TestOpts.(*engine.TestOpts).DBOpts.ConnString)
			if err != nil {
				b.Exit(err)
			}

			switch dialectName {
			case db.MYSQL:
				return "_data_f0f0f0 LIKE '%eedl%' AND id > " + strconv.FormatUint(id, 10)
			case db.POSTGRES:
				return "json_data->'field0'->'field0'->>'field0' LIKE '%eedl%' AND id > " + strconv.FormatUint(id, 10) // searching for the 'needle' word
			default:
				b.Exit("The %s test is not supported on driver: %s", testDesc.Name, dialectName)
			}

			return ""
		}
		orderby := func(worker *benchmark.BenchmarkWorker) string { //nolint:revive
			return "id ASC"
		}
		engine.TestSelectRawSQLQuery(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSelectJSONByNonIndexedValue selects a row from the 'json' table by some json condition
var TestSelectJSONByNonIndexedValue = engine.TestDesc{
	Name:        "select-json-by-nonindexed-value",
	Metric:      "rows/sec",
	Description: "select a row from the 'json' table by some json condition",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.MYSQL, db.POSTGRES},
	Table:       TestTableJSON,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		where := func(worker *benchmark.BenchmarkWorker) string {
			id := worker.Randomizer.Uintn64(testDesc.Table.RowsCount - 1)

			var dialectName, err = db.GetDialectName(b.TestOpts.(*engine.TestOpts).DBOpts.ConnString)
			if err != nil {
				b.Exit(err)
			}

			switch dialectName {
			case db.MYSQL:
				return "JSON_EXTRACT(json_data, '$.field0.field1') = '10' AND id > " + strconv.FormatUint(id, 10)
			case db.POSTGRES:
				return "json_data @> '{\"field0\": {\"field1\": 10}}' AND id > " + strconv.FormatUint(id, 10)
			default:
				b.Exit("The %s test is not supported on driver: %s", testDesc.Name, dialectName)
			}

			return ""
		}
		orderby := func(b *benchmark.BenchmarkWorker) string { //nolint:revive
			return "id ASC"
		}
		engine.TestSelectRawSQLQuery(b, testDesc, nil, "id", where, orderby, 1)
	},
}

// TestSearchJSONByNonIndexedValue searches a row from the 'json' table using some json condition using LIKE {}
var TestSearchJSONByNonIndexedValue = engine.TestDesc{
	Name:        "search-json-by-nonindexed-value",
	Metric:      "rows/sec",
	Description: "search a row from the 'json' table using some json condition using LIKE {}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.MYSQL, db.POSTGRES},
	Table:       TestTableJSON,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		where := func(worker *benchmark.BenchmarkWorker) string {
			id := worker.Randomizer.Uintn64(testDesc.Table.RowsCount - 1)

			var dialectName, err = db.GetDialectName(b.TestOpts.(*engine.TestOpts).DBOpts.ConnString)
			if err != nil {
				b.Exit(err)
			}

			switch dialectName {
			case db.MYSQL:
				return "JSON_EXTRACT(json_data, '$.field0.field1') LIKE '%eedl%' AND id > " + strconv.FormatUint(id, 10)
			case db.POSTGRES:
				return "json_data->'field0'->'field0'->>'field0' LIKE '%eedl%' AND id > " + strconv.FormatUint(id, 10) // searching for the 'needle' word
			default:
				b.Exit("The %s test is not supported on driver: %s", testDesc.Name, dialectName)
			}

			return ""
		}
		orderby := func(worker *benchmark.BenchmarkWorker) string { //nolint:revive
			return "id ASC"
		}
		engine.TestSelectRawSQLQuery(b, testDesc, nil, "id", where, orderby, 1)
	},
}
