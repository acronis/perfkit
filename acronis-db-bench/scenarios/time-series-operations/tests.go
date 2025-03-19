package time_series_operations

import (
	"fmt"

	"github.com/acronis/perfkit/benchmark"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

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

		where := func(b *benchmark.Benchmark, workerId int) string {
			w := b.GenFakeDataAsMap(workerId, colConfs, false)

			return fmt.Sprintf("tenant_id = '%s' AND device_id = '%s' AND metric_id = '%s'", (*w)["tenant_id"], (*w)["device_id"], (*w)["metric_id"])
		}
		orderby := func(b *benchmark.Benchmark) string { //nolint:revive
			return "id DESC"
		}

		engine.TestSelectRawSQLQuery(b, testDesc, nil, "id", where, orderby, 1)

		b.Vault.(*engine.DBTestData).EffectiveBatch = origBatch
	},
}
