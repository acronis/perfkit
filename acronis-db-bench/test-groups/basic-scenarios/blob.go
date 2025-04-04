package basic_scenarios

import (
	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableBlob is table to store blobs
var TestTableBlob = engine.TestTable{
	TableName: "acronis_db_bench_blob",
	Databases: engine.ALL,
	Columns: [][]interface{}{
		{"id", "autoinc"},
		{"uuid", "uuid"},
		{"tenant_id", "tenant_uuid"},
		{"timestamp", "time_ns"},
		{"data", "blob"},
	},
	InsertColumns: []string{}, // all
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
				{Name: "uuid", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				{Name: "tenant_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				{Name: "timestamp", Type: db.DataTypeBigInt, NotNull: true, Indexed: true},
				{Name: "data", Type: db.DataTypeHugeBlob, NotNull: true},
			},
		}
	},
	CreateQuery: `create table {table} (
		id {$bigint_autoinc_pk},
		uuid {$varchar_uuid} {$notnull},
		tenant_id {$varchar_uuid} {$notnull},
		timestamp bigint {$notnull},
		data {$hugeblob} {$notnull}
		) {$engine};`,
	Indexes: [][]string{{"tenant_id"}, {"uuid"}},
}

// TestInsertBlob inserts a row with large random blob into the 'blob' table
var TestInsertBlob = engine.TestDesc{
	Name:        "insert-blob",
	Metric:      "rows/sec",
	Description: "insert a row with large random blob into the 'blob' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       TestTableBlob,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testDesc.Table.InitColumnsConf()
		for i := range testDesc.Table.ColumnsConf {
			if testDesc.Table.ColumnsConf[i].ColumnType == "blob" {
				testDesc.Table.ColumnsConf[i].MaxSize = b.TestOpts.(*engine.TestOpts).TestcaseOpts.MaxBlobSize
				testDesc.Table.ColumnsConf[i].MinSize = b.TestOpts.(*engine.TestOpts).TestcaseOpts.MinBlobSize
			}
		}
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestCopyBlob copies a row with large random blob into the 'blob' table
var TestCopyBlob = engine.TestDesc{
	Name:        "copy-blob",
	Metric:      "rows/sec",
	Description: "copy a row with large random blob into the 'blob' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableBlob,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		testDesc.Table.InitColumnsConf()
		for i := range testDesc.Table.ColumnsConf {
			if testDesc.Table.ColumnsConf[i].ColumnType == "blob" {
				testDesc.Table.ColumnsConf[i].MaxSize = b.TestOpts.(*engine.TestOpts).TestcaseOpts.MaxBlobSize
				testDesc.Table.ColumnsConf[i].MinSize = b.TestOpts.(*engine.TestOpts).TestcaseOpts.MinBlobSize
			}
		}
		engine.TestGeneric(b, testDesc, copyDataWorker, 0)
	},
}

// TestSelectBlobLastTenant is the same as TestSelectBlobLast but with tenant-awareness
var TestSelectBlobLastTenant = engine.TestDesc{
	Name:        "select-blob-last-in-tenant",
	Metric:      "rows/sec",
	Description: "select the last row from the 'blob' table WHERE tenant_id = {random tenant uuid}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       TestTableBlob,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		worker := func(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
			return tenantAwareWorker(b, c, testDesc, "ORDER BY timestamp DESC", 1)
		}
		engine.TestGeneric(b, testDesc, worker, 1)
	},
}
