package logs_search

import (
	"fmt"
	"time"

	"github.com/acronis/perfkit/benchmark"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
	basic_scenarios "github.com/acronis/perfkit/acronis-db-bench/scenarios/basic-scenarios"
)

func init() {
	tests := []*engine.TestDesc{
		// Logs search tests
		&TestSelectHeavyRandPageByUUID,
		&TestSelectHeavyRandCustomerRecent,
		&TestSelectHeavyRandCustomerRecentLike,
		&TestSelectHeavyRandCustomerUpdateTimePage,
		&TestSelectHeavyRandCustomerCount,
		&TestSelectHeavyRandPartnerRecent,
		&TestSelectHeavyRandPartnerStartUpdateTimePage,
	}

	tables := map[string]engine.TestTable{
		basic_scenarios.TestTableHeavy.TableName: basic_scenarios.TestTableHeavy,
	}

	scenario := &engine.TestScenario{
		Name:   "logs-search",
		Tests:  tests,
		Tables: tables,
	}

	if err := engine.RegisterTestScenario(scenario); err != nil {
		panic(err)
	}
}

// TestSelectHeavyRandPageByUUID selects random N rows from the 'heavy' table WHERE uuid IN (...)
var TestSelectHeavyRandPageByUUID = engine.TestDesc{
	Name:        "select-heavy-rand-page-by-uuid",
	Metric:      "rows/sec",
	Description: "select page from the 'heavy' table WHERE uuid IN (...)",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       basic_scenarios.TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {

		var batchSize = b.Vault.(*engine.DBTestData).EffectiveBatch

		var colConfs []benchmark.DBFakeColumnConf
		for i := 0; i < batchSize; i++ {
			colConfs = append(colConfs, benchmark.DBFakeColumnConf{ColumnName: "uuid", ColumnType: "uuid"})
		}

		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			_, values, err := worker.Randomizer.GenFakeData(&colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			var valuesToSearch []string
			for _, v := range values {
				valuesToSearch = append(valuesToSearch, fmt.Sprintf("%s", v))
			}

			return map[string][]string{"uuid": valuesToSearch}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, nil, 1)
	},
}

// TestSelectHeavyRandCustomerRecent selects random page from the 'heavy' table WHERE tenant_id = {} AND ordered by enqueue_time DESC
var TestSelectHeavyRandCustomerRecent = engine.TestDesc{
	Name:        "select-heavy-rand-in-customer-recent",
	Metric:      "rows/sec",
	Description: "select first page from the 'heavy' table WHERE tenant_id = {} ORDER BY enqueue_time DESC",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       basic_scenarios.TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {

		// colConfs := testDesc.table.GetColumnsConf([]string{"tenant_id"}, false)

		var colConfs = &[]benchmark.DBFakeColumnConf{{ColumnName: "customer_id", ColumnType: "customer_uuid"}}

		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			return map[string][]string{
				"tenant_vis_list": {fmt.Sprintf("%s", (*w)["customer_id"])},
			}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"desc(enqueue_time)"}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectHeavyRandCustomerRecentLike selects random page from the 'heavy' table WHERE tenant_id = {} AND policy_name LIKE '%k%' AND ordered by enqueue_time DESC
var TestSelectHeavyRandCustomerRecentLike = engine.TestDesc{
	Name:        "select-heavy-rand-in-customer-recent-like",
	Metric:      "rows/sec",
	Description: "select first page from the 'heavy' table WHERE tenant_id = {} AND policy_name LIKE '%k%' ORDER BY enqueue_time DESC",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       basic_scenarios.TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {

		// colConfs := testDesc.table.GetColumnsConf([]string{"tenant_id"}, false)

		var colConfs = &[]benchmark.DBFakeColumnConf{{ColumnName: "customer_id", ColumnType: "customer_uuid"}}

		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			return map[string][]string{
				"tenant_vis_list": {fmt.Sprintf("%s", (*w)["customer_id"])},
				"policy_name":     {"like(k)"},
			}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"desc(enqueue_time)"}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectHeavyRandCustomerUpdateTimePage selects random page from the 'heavy' table WHERE tenant_id = {} AND ordered by enqueue_time DESC
var TestSelectHeavyRandCustomerUpdateTimePage = engine.TestDesc{
	Name:        "select-heavy-rand-customer-update-time-page",
	Metric:      "rows/sec",
	Description: "select first page from the 'heavy' table WHERE customer_id = {} AND update_time_ns in 1h interval ORDER BY update_time DESC",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       basic_scenarios.TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {

		var colConfs = []benchmark.DBFakeColumnConf{{ColumnName: "customer_id", ColumnType: "customer_uuid"}}
		colConfs = append(colConfs, benchmark.DBFakeColumnConf{ColumnName: "update_time", ColumnType: "time", Cardinality: 30})

		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(&colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			var pageStart = (*w)["update_time"].(time.Time)
			var pageEnd = pageStart.Add(time.Hour)

			return map[string][]string{
				"tenant_vis_list": {fmt.Sprintf("%s", (*w)["customer_id"])},
				"update_time": {
					fmt.Sprintf("ge(%s)", pageStart.Format(time.RFC3339)),
					fmt.Sprintf("lt(%s)", pageEnd.Format(time.RFC3339)),
				},
			}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"asc(update_time)"}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectHeavyRandCustomerCount selects random count of rows from the 'heavy' table WHERE tenant_id = {}
var TestSelectHeavyRandCustomerCount = engine.TestDesc{
	Name:        "select-heavy-rand-in-customer-count",
	Metric:      "rows/sec",
	Description: "select COUNT(0) from the 'heavy' table WHERE tenant_id = {}",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       basic_scenarios.TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var colConfs = &[]benchmark.DBFakeColumnConf{{ColumnName: "customer_id", ColumnType: "customer_uuid"}}

		var countToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			return map[string][]string{
				"tenant_vis_list": {fmt.Sprintf("%s", (*w)["customer_id"])},
			}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"COUNT(0)"}, []interface{}{&countToRead}, where, nil, 1)
	},
}

// TestSelectHeavyRandPartnerRecent selects random page from the 'heavy' table WHERE tenant_id = {} AND ordered by enqueue_time DESC
var TestSelectHeavyRandPartnerRecent = engine.TestDesc{
	Name:        "select-heavy-rand-in-partner-recent",
	Metric:      "rows/sec",
	Description: "select first page from the 'heavy' table WHERE partner_id = {} ORDER BY enqueue_time DESC",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       basic_scenarios.TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var colConfs = &[]benchmark.DBFakeColumnConf{{ColumnName: "partner_id", ColumnType: "partner_uuid"}}

		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			return map[string][]string{
				"tenant_vis_list": {fmt.Sprintf("%s", (*w)["partner_id"])},
			}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"desc(enqueue_time)"}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectHeavyRandPartnerStartUpdateTimePage selects random page from the 'heavy' table WHERE tenant_id = {} AND ordered by enqueue_time DESC
var TestSelectHeavyRandPartnerStartUpdateTimePage = engine.TestDesc{
	Name:        "select-heavy-rand-partner-start-update-time-page",
	Metric:      "rows/sec",
	Description: "select first page from the 'heavy' table WHERE partner_id = {} ORDER BY enqueue_time DESC",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	Table:       basic_scenarios.TestTableHeavy,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var colConfs = []benchmark.DBFakeColumnConf{{ColumnName: "partner_id", ColumnType: "partner_uuid"}}
		colConfs = append(colConfs, benchmark.DBFakeColumnConf{ColumnName: "update_time", ColumnType: "time", Cardinality: 30})

		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string {
			w, err := worker.Randomizer.GenFakeDataAsMap(&colConfs, false)
			if err != nil {
				worker.Exit(err)
			}

			var pageStart = (*w)["update_time"].(time.Time)
			var pageEnd = pageStart.Add(2 * 24 * time.Hour)

			var pageStartStr = pageStart.Add(-time.Hour)

			return map[string][]string{
				"tenant_vis_list": {fmt.Sprintf("%s", (*w)["partner_id"])},
				"update_time": {
					fmt.Sprintf("ge(%s)", pageStart.Format(time.RFC3339)),
					fmt.Sprintf("lt(%s)", pageEnd.Format(time.RFC3339)),
				},
				"start_time": {
					fmt.Sprintf("ge(%s)", pageStartStr.Format(time.RFC3339)),
				},
			}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"asc(update_time)"}
		}

		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}
