package time_series_operations

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Time series operations tests group")

	// Time series SQL tests
	tg.Add(&TestInsertTimeSeriesSQL)
	tg.Add(&TestSelectTimeSeriesSQL)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

// TestTableTimeSeriesSQL is table to store time series data
var TestTableTimeSeriesSQL = engine.TestTable{
	TableName: "acronis_db_bench_ts_sql",
	Databases: engine.ALL,
	Columns: [][]interface{}{
		{"id", "autoinc", 0},
		{"tenant_id", "tenant_uuid", 0},
		{"device_id", "tenant_uuid_bound_id", 50}, // up to 50 devices per tenant
		{"metric_id", "cti_uuid", 10},             // up to 10 metrics to be used per every device
		{"ts", "now", 0},
		{"value", "int", 100},
	},
	InsertColumns: []string{}, // all
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		return &db.TableDefinition{
			TableRows: []db.TableRow{
				{Name: "id", Type: db.DataTypeBigIntAutoIncPK},
				{Name: "tenant_id", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "device_id", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "metric_id", Type: db.DataTypeVarCharUUID, NotNull: true, Indexed: true},
				{Name: "ts", Type: db.DataTypeTimestamp, NotNull: true, Indexed: true},
				{Name: "value", Type: db.DataTypeInt, NotNull: true},
			},
		}
	},
	CreateQuery: `create table acronis_db_bench_ts_sql(
			id {$bigint_autoinc_pk},
			tenant_id {$varchar_uuid} {$notnull},
			device_id {$tenant_uuid_bound_id} {$notnull},
			metric_id {$varchar_uuid} {$notnull},
			ts timestamp {$notnull},
			value int {$notnull}
		) {$engine};`,
	CreateQueryPatchFuncs: []engine.CreateQueryPatchFunc{
		func(table string, query string, dialect db.DialectName) (string, error) { //nolint:revive
			if dialect == db.CASSANDRA {
				query = strings.ReplaceAll(query, string(db.DataTypeBigIntAutoIncPK), string(db.DataTypeBigIntAutoInc))
				query = strings.ReplaceAll(query, "value int {$notnull}", `value int,
						PRIMARY KEY ((tenant_id, device_id, metric_id), id, ts)
					`)
			}

			return query, nil
		},
	},
	Indexes: [][]string{{"tenant_id"}, {"device_id"}, {"metric_id"}},
}

// TestInsertTimeSeriesSQL inserts into the 'timeseries' SQL table
var TestInsertTimeSeriesSQL = engine.TestDesc{
	Name:        "insert-ts-sql",
	Metric:      "values/sec",
	Description: "batch insert into the 'timeseries' SQL table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.PMWSA,
	Table:       TestTableTimeSeriesSQL,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {

		origBatch := b.Vault.(*engine.DBTestData).EffectiveBatch
		if b.TestOpts.(*engine.TestOpts).BenchOpts.Batch == 0 {
			b.Vault.(*engine.DBTestData).EffectiveBatch = 256
		}

		engine.TestInsertGeneric(b, testDesc)

		b.Vault.(*engine.DBTestData).EffectiveBatch = origBatch
	},
}

// TestSelectTimeSeriesSQL selects last inserted row from the 'timeseries' SQL table
var TestSelectTimeSeriesSQL = engine.TestDesc{
	Name:        "select-ts-sql",
	Metric:      "values/sec",
	Description: "batch select from the 'timeseries' SQL table",
	Category:    engine.TestSelect,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   engine.PMWSA,
	Table:       TestTableTimeSeriesSQL,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {

		origBatch := b.Vault.(*engine.DBTestData).EffectiveBatch
		if b.TestOpts.(*engine.TestOpts).BenchOpts.Batch == 0 {
			b.Vault.(*engine.DBTestData).EffectiveBatch = 256
		}

		colConfs := testDesc.Table.GetColumnsConf([]string{"tenant_id", "device_id", "metric_id"}, false)

		where := func(worker *benchmark.BenchmarkWorker) string {
			w, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			return fmt.Sprintf("tenant_id = '%s' AND device_id = '%s' AND metric_id = '%s'", (*w)["tenant_id"], (*w)["device_id"], (*w)["metric_id"])
		}
		orderby := func(worker *benchmark.BenchmarkWorker) string { //nolint:revive
			return "id DESC"
		}

		engine.TestSelectRawSQLQuery(b, testDesc, nil, "id", where, orderby, 1)

		b.Vault.(*engine.DBTestData).EffectiveBatch = origBatch
	},
}
