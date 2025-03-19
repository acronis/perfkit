package json_search

import (
	"strconv"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

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
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.Table.RowsCount - 1)

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
		orderby := func(b *benchmark.Benchmark) string { //nolint:revive
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
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.Table.RowsCount - 1)

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
		orderby := func(b *benchmark.Benchmark) string { //nolint:revive
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
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.Table.RowsCount - 1)

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
		orderby := func(b *benchmark.Benchmark) string { //nolint:revive
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
		where := func(b *benchmark.Benchmark, workerId int) string {
			id := b.Randomizer.GetWorker(workerId).Uintn64(testDesc.Table.RowsCount - 1)

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
		orderby := func(b *benchmark.Benchmark) string { //nolint:revive
			return "id ASC"
		}
		engine.TestSelectRawSQLQuery(b, testDesc, nil, "id", where, orderby, 1)
	},
}
