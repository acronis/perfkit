package basic_scenarios

import (
	"fmt"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableMedium is table to store medium objects
var TestTableMedium = engine.TestTable{
	TableName: "acronis_db_bench_medium",
	Databases: engine.ALL,
	Columns: [][]interface{}{
		{"id", "autoinc"},
		{"uuid", "uuid"},
		{"tenant_id", "tenant_uuid"},
		{"euc_id", "int", 2147483647},
		{"progress", "int", 100},
	},
	InsertColumns: []string{}, // all
	UpdateColumns: []string{"progress"},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
				{Name: "uuid", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "tenant_id", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "euc_id", Type: db.DataTypeInt, NotNull: true, Indexed: true},
				{Name: "progress", Type: db.DataTypeInt},
			},
		}
	},
	CreateQuery: `create table {table} (
			id {$bigint_autoinc_pk},
			tenant_id {$varchar_uuid} {$notnull},
			uuid {$varchar_uuid} {$notnull},
			euc_id int {$notnull},
			progress int {$null}
			) {$engine};`,
	Indexes: [][]string{{"tenant_id"}},
}

// TestInsertMedium inserts a row into the 'medium' table
var TestInsertMedium = engine.TestDesc{
	Name:        "insert-medium",
	Metric:      "rows/sec",
	Description: "insert a row into the 'medium' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertMediumPrepared inserts a row into the 'medium' table using prepared statement for the batch
var TestInsertMediumPrepared = engine.TestDesc{
	Name:        "insert-medium-prepared",
	Metric:      "rows/sec",
	Description: "insert a row into the 'medium' table using prepared statement for the batch",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, insertByPreparedDataWorker, 0)
	},
}

// TestInsertMediumMultiValue inserts a row into the 'medium' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...)
var TestInsertMediumMultiValue = engine.TestDesc{
	Name:        "insert-medium-multivalue",
	Metric:      "rows/sec",
	Description: "insert a row into the 'medium' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...) ",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.PMWSA,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, engine.InsertMultiValueDataWorker, 0)
	},
}

// TestCopyMedium copies a row into the 'medium' table
var TestCopyMedium = engine.TestDesc{
	Name:        "copy-medium",
	Metric:      "rows/sec",
	Description: "copy a row into the 'medium' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, copyDataWorker, 0)
	},
}

// TestInsertMediumDBR inserts a row into the 'medium' table using goland DBR query builder
var TestInsertMediumDBR = engine.TestDesc{
	Name:        "dbr-insert-medium",
	Metric:      "rows/sec",
	Description: "insert a row into the 'medium' table using goland DBR query builder",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestUpdateMedium updates random row in the 'medium' table
var TestUpdateMedium = engine.TestDesc{
	Name:        "update-medium",
	Metric:      "rows/sec",
	Description: "update random row in the 'medium' table",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestUpdateGeneric(b, testDesc, 1, nil)
	},
}

// TestUpdateMediumDBR updates random row in the 'medium' table using golang DBR query builder
var TestUpdateMediumDBR = engine.TestDesc{
	Name:        "dbr-update-medium",
	Metric:      "rows/sec",
	Description: "update random row in the 'medium' table using golang DB driver",
	Category:    engine.TestUpdate,
	IsReadonly:  false,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestUpdateGeneric(b, testDesc, 1, nil)
	},
}

// TestSelectMediumLastTenant is the same as TestSelectMediumLast but with tenant-awareness
var TestSelectMediumLastTenant = engine.TestDesc{
	Name:        "select-medium-last-in-tenant",
	Metric:      "rows/sec",
	Description: "select the last row from the 'medium' table WHERE tenant_id = {random tenant uuid}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		worker := func(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
			return tenantAwareWorker(b, c, testDesc, "ORDER BY enqueue_time DESC", 1)
		}
		engine.TestGeneric(b, testDesc, worker, 1)
	},
}

// TestSelectMediumLast tests select last row from the 'medium' table with few columns and 1 index
var TestSelectMediumLast = engine.TestDesc{
	Name:        "select-medium-last",
	Metric:      "rows/sec",
	Description: "select last row from the 'medium' table with few columns and 1 index",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64
		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { return []string{"desc(id)"} } //nolint:revive
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, nil, orderBy, 1)
	},
}

// TestSelectMediumLastDBR tests select last row from the 'medium' table with few columns and 1 index using golang DBR query builder
var TestSelectMediumLastDBR = engine.TestDesc{
	Name:        "dbr-select-medium-last",
	Metric:      "rows/sec",
	Description: "select last row from the 'medium' table with few columns and 1 index",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64
		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { return []string{"desc(id)"} } //nolint:revive
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, nil, orderBy, 1)
	},
}

// TestSelectMediumRand selects random row from the 'medium' table with few columns and 1 index
var TestSelectMediumRand = engine.TestDesc{
	Name:        "select-medium-rand",
	Metric:      "rows/sec",
	Description: "select random row from the 'medium' table with few columns and 1 index",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			id := worker.Randomizer.Uintn64(testDesc.Table.RowsCount - 1)

			return map[string][]string{"id": {fmt.Sprintf("ge(%d)", id)}}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"asc(id)"}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectMediumRandDBR selects random row from the 'medium' table using golang DBR query builder
var TestSelectMediumRandDBR = engine.TestDesc{
	Name:        "dbr-select-medium-rand",
	Metric:      "rows/sec",
	Description: "select random row from the 'medium' table using golang DBR query builder",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   true,
	Databases:   engine.RELATIONAL,
	Table:       TestTableMedium,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			var id = worker.Randomizer.Uintn64(testDesc.Table.RowsCount - 1)

			return map[string][]string{"id": {fmt.Sprintf("gt(%d)", id)}}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"asc(id)"}
		}
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}
