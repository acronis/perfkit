package logs_search

import (
	"fmt"
	"time"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Logs search tests group")

	// Logs search tests
	tg.Add(&TestSelectHeavyRandPageByUUID)
	tg.Add(&TestSelectHeavyRandCustomerRecent)
	tg.Add(&TestSelectHeavyRandCustomerRecentLike)
	tg.Add(&TestSelectHeavyRandCustomerUpdateTimePage)
	tg.Add(&TestSelectHeavyRandCustomerCount)
	tg.Add(&TestSelectHeavyRandPartnerRecent)
	tg.Add(&TestSelectHeavyRandPartnerStartUpdateTimePage)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

var tableLogsSchema = `
	id {$bigint_autoinc_pk},
	uuid                      {$uuid}        not null {$unique},
	checksum                  varchar(64) not null,
	tenant_id                 varchar(36) not null, -- conditional, high cardinality (about 100K), no empty values
	cti_entity_uuid           varchar(36),
	euc_id                    varchar(64) not null, -- conditional, high cardinality (about 100K)
	workflow_id               bigint,
	state                     integer     not null, -- conditional, orderable, small cardinality (around 5), no empty values
	type                      varchar(64) not null, -- conditional, orderable, small cardinality (around 100), no empty values
	queue                     varchar(64) not null, -- conditional, small cardinality (around 100), no empty values
	priority                  integer     not null, -- conditional, orderable, small cardinality (around 5), no empty values
	issuer_id                 varchar(64) not null,
	issuer_cluster_id         varchar(64),
	heartbeat_ivl_str         varchar(64),
	heartbeat_ivl_ns          bigint,
	queue_timeout_str         varchar(64),
	queue_timeout_ns          bigint,
	ack_timeout_str           varchar(64),
	ack_timeout_ns            bigint,
	exec_timeout_str          varchar(64),
	exec_timeout_ns           bigint,
	life_time_str             varchar(64),
	life_time_ns              bigint,
	max_assign_count          integer     not null,
	assign_count              integer     not null,
	max_fail_count            integer     not null,
	fail_count                integer     not null,
	cancellable               {$boolean}     not null,
	cancel_requested          {$boolean}     not null,
	blocker_count             integer     not null,
	started_by_user           varchar(256),         -- conditional, orderable, high cardinality (about 100K)
	policy_id                 varchar(64),          -- conditional, high cardinality (about 100K)
	policy_type               varchar(64),          -- conditional, high cardinality (about 100K)
	policy_name               varchar(256),         -- conditional, high cardinality (about 100K)
	resource_id               varchar(64),          -- conditional, high cardinality (about 100K)
	resource_type             varchar(64),          -- conditional, high cardinality (about 100K)
	resource_name             varchar(256),         -- conditional, orderable, high cardinality (about 100K)
	tags                      text,
	affinity_agent_id         varchar(64) not null,
	affinity_cluster_id       varchar(64) not null,
	argument                  {$binaryblobtype},
	context                   {$binaryblobtype},
	progress                  integer,
	progress_total            integer,
	assigned_agent_id         varchar(64),
	assigned_agent_cluster_id varchar(64),
	enqueue_time_str          varchar(64),
	enqueue_time_ns           bigint      not null, -- conditional, orderable, unique
	assign_time_str           varchar(64),
	assign_time_ns            bigint,               -- conditional, orderable, unique
	start_time_str            varchar(64),
	start_time_ns             bigint,               -- conditional, orderable, unique
	update_time_str           varchar(64) not null,
	update_time_ns            bigint      not null, -- conditional, orderable, unique
	completion_time_str       varchar(64),
	completion_time_ns        bigint,               -- conditional, orderable, unique
	result_code               integer,              -- conditional, orderable, small cardinality (around 5)
	result_error              {$binaryblobtype},
	result_warnings           {$binaryblobtype},
	result_payload            {$binaryblobtype},
	const_val                 integer               -- special data to test update of the same data
`

// TestTableLogs is table to store logs objects
var TestTableLogs = engine.TestTable{
	TableName: "acronis_db_bench_logs",
	Databases: engine.ALL,
	Columns: [][]interface{}{
		{"id", "autoinc"},
		{"uuid", "uuid", 0},
		{"checksum", "string", 0, 64},
		{"cti_entity_uuid", "cti_uuid", 0},

		{"tenant_id", "tenant_uuid", 0},
		{"euc_id", "string", 0, 64},

		// {"tenant_vis_list", "tenant_uuid_parents", 0},

		{"workflow_id", "int", 2147483647},
		{"state", "int", 16},
		{"type", "string", 256, 64},
		{"queue", "string", 256, 64},
		{"priority", "int", 5},

		{"issuer_id", "uuid", 0},

		{"max_assign_count", "int", 100},
		{"assign_count", "int", 5},
		{"cancellable", "bool", 0},
		{"cancel_requested", "bool", 0},
		{"blocker_count", "int", 3},

		{"started_by_user", "string", 0, 32},

		{"policy_id", "int", 1024},
		{"policy_type", "string", 1024, 64},
		{"policy_name", "string", 16384, 256},

		{"resource_id", "uuid", 0},
		{"resource_type", "int", 256},
		{"resource_name", "string", 0, 256},

		{"affinity_agent_id", "uuid", 0},
		{"affinity_cluster_id", "string", 0, 32},

		{"progress", "int", 100},
		{"progress_total", "int", 100},

		{"enqueue_time", "time_ns", 0},
		{"assign_time", "time_ns", 0},
		{"start_time", "time_ns", 0},
		{"update_time", "time_ns", 0},
		{"completion_time", "time_ns", 0},

		{"result_code", "int", 32},
		{"result_payload", "rbyte", 0, 256},

		{"const_val", "int", 1},
	},
	InsertColumns: []string{}, // all
	UpdateColumns: []string{"progress", "result_payload", "update_time", "completion_time"},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		var tableRows []db.TableRow

		tableRows = append(tableRows,
			db.TableRowItem{Name: "id", Type: db.DataTypeBigIntAutoIncPK, Indexed: true},
			db.TableRowItem{Name: "uuid", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
			db.TableRowItem{Name: "checksum", Type: db.DataTypeVarChar64, NotNull: true},
			db.TableRowItem{Name: "cti_entity_uuid", Type: db.DataTypeVarChar36, Indexed: true},
		)

		if dialect == db.CLICKHOUSE {
			// Needed for primary key
			tableRows = append(tableRows,
				db.TableRowItem{Name: "partner_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				db.TableRowItem{Name: "customer_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				db.TableRowItem{Name: "tenant_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
			)
		} else if dialect == db.ELASTICSEARCH {
			tableRows = append(tableRows,
				db.TableRowItem{Name: "tenant_id", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
				db.TableRowItem{Name: "tenant_vis_list", Type: db.DataTypeUUID, NotNull: true, Indexed: true},
			)
		} else {
			tableRows = append(tableRows,
				db.TableRowItem{Name: "tenant_id", Type: db.DataTypeVarChar36, NotNull: true, Indexed: true},
				db.TableRowItem{Name: "euc_id", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			)
		}

		tableRows = append(tableRows,
			db.TableRowItem{Name: "workflow_id", Type: db.DataTypeBigInt, NotNull: true, Indexed: true},
			db.TableRowItem{Name: "state", Type: db.DataTypeInt, NotNull: true, Indexed: true},
			db.TableRowItem{Name: "type", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			db.TableRowItem{Name: "queue", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			db.TableRowItem{Name: "priority", Type: db.DataTypeInt, NotNull: true, Indexed: true},

			db.TableRowItem{Name: "issuer_id", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			db.TableRowItem{Name: "issuer_cluster_id", Type: db.DataTypeVarChar64, Indexed: true},

			db.TableRowItem{Name: "heartbeat_ivl", Type: db.DataTypeBigInt},
			db.TableRowItem{Name: "queue_timeout", Type: db.DataTypeBigInt},
			db.TableRowItem{Name: "ack_timeout", Type: db.DataTypeBigInt},
			db.TableRowItem{Name: "exec_timeout", Type: db.DataTypeBigInt},
			db.TableRowItem{Name: "life_time", Type: db.DataTypeBigInt},

			db.TableRowItem{Name: "max_assign_count", Type: db.DataTypeInt, NotNull: true},
			db.TableRowItem{Name: "assign_count", Type: db.DataTypeInt, NotNull: true},
			db.TableRowItem{Name: "cancellable", Type: db.DataTypeBoolean, NotNull: true},
			db.TableRowItem{Name: "cancel_requested", Type: db.DataTypeBoolean, NotNull: true},
			db.TableRowItem{Name: "blocker_count", Type: db.DataTypeInt, NotNull: true},

			db.TableRowItem{Name: "started_by_user", Type: db.DataTypeVarChar256, Indexed: true},
		)

		if dialect == db.CASSANDRA {
			tableRows = append(tableRows, db.TableRowItem{Name: "policy_id", Type: db.DataTypeInt, Indexed: true})
		} else {
			tableRows = append(tableRows, db.TableRowItem{Name: "policy_id", Type: db.DataTypeVarChar64, Indexed: true})
		}

		tableRows = append(tableRows,
			db.TableRowItem{Name: "policy_type", Type: db.DataTypeVarChar64, Indexed: true},
			db.TableRowItem{Name: "policy_name", Type: db.DataTypeVarChar256, Indexed: true},

			db.TableRowItem{Name: "resource_id", Type: db.DataTypeVarChar64, Indexed: true},
		)

		if dialect == db.CASSANDRA {
			tableRows = append(tableRows, db.TableRowItem{Name: "resource_type", Type: db.DataTypeInt, Indexed: true})
		} else {
			tableRows = append(tableRows, db.TableRowItem{Name: "resource_type", Type: db.DataTypeVarChar64, Indexed: true})
		}

		tableRows = append(tableRows,
			db.TableRowItem{Name: "resource_name", Type: db.DataTypeVarChar256, Indexed: true},

			db.TableRowItem{Name: "tags", Type: db.DataTypeText, Indexed: true},

			db.TableRowItem{Name: "affinity_agent_id", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},
			db.TableRowItem{Name: "affinity_cluster_id", Type: db.DataTypeVarChar64, NotNull: true, Indexed: true},

			db.TableRowItem{Name: "argument", Type: db.DataTypeBinaryBlobType},
			db.TableRowItem{Name: "context", Type: db.DataTypeBinaryBlobType},

			db.TableRowItem{Name: "progress", Type: db.DataTypeInt},
			db.TableRowItem{Name: "progress_total", Type: db.DataTypeInt},

			db.TableRowItem{Name: "assigned_agent_id", Type: db.DataTypeVarChar64, Indexed: true},
			db.TableRowItem{Name: "assigned_agent_cluster_id", Type: db.DataTypeVarChar64, Indexed: true},

			db.TableRowItem{Name: "enqueue_time", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRowItem{Name: "assign_time", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRowItem{Name: "start_time", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRowItem{Name: "update_time", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRowItem{Name: "completion_time", Type: db.DataTypeBigInt, Indexed: true},

			db.TableRowItem{Name: "result_code", Type: db.DataTypeInt, Indexed: true},
			db.TableRowItem{Name: "result_error", Type: db.DataTypeBinaryBlobType},
			db.TableRowItem{Name: "result_warnings", Type: db.DataTypeBinaryBlobType},
			db.TableRowItem{Name: "result_payload", Type: db.DataTypeBinaryBlobType},

			db.TableRowItem{Name: "const_val", Type: db.DataTypeInt},
		)

		var tableDef = &db.TableDefinition{
			TableRows: tableRows,
		}
		if dialect == db.CLICKHOUSE {
			tableDef.PrimaryKey = []string{"partner_id", "customer_id", "toDate(update_time)"}
		}

		if dialect == db.ELASTICSEARCH {
			tableDef.Resilience.NumberOfReplicas = 2
		}

		return tableDef
	},
	CreateQuery: `create table {table} (` + tableLogsSchema + `) {$engine};`,
	Indexes: [][]string{
		{"uuid"},
		{"completion_time"},
		{"cti_entity_uuid"},
		{"tenant_id"},
		{"euc_id"},
		{"queue", "state", "affinity_agent_id", "affinity_cluster_id", "tenant_id", "priority"},
		{"queue", "state", "affinity_agent_id", "affinity_cluster_id", "euc_id", "priority"},
		{"update_time"},
		{"state", "completion_time"},
		{"start_time"},
		{"enqueue_time"},
		{"resource_id"},
		{"policy_id"},
		{"result_code"},
		{"resource_id", "enqueue_time"},
		{"type"},
		{"type", "tenant_id", "enqueue_time"},
		{"type", "euc_id", "enqueue_time"},
		{"queue", "type", "tenant_id"},
		{"queue", "type", "euc_id"},
	},
}

// TestSelectHeavyRandPageByUUID selects random N rows from the 'heavy' table WHERE uuid IN (...)
var TestSelectHeavyRandPageByUUID = engine.TestDesc{
	Name:        "select-heavy-rand-page-by-uuid",
	Metric:      "rows/sec",
	Description: "select page from the 'heavy' table WHERE uuid IN (...)",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	Databases:   engine.ALL,
	Table:       TestTableLogs,
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
	Databases:   engine.ALL,
	Table:       TestTableLogs,
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
	Databases:   engine.ALL,
	Table:       TestTableLogs,
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
	Databases:   engine.ALL,
	Table:       TestTableLogs,
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
	Databases:   engine.ALL,
	Table:       TestTableLogs,
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
	Databases:   engine.ALL,
	Table:       TestTableLogs,
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
	Databases:   engine.ALL,
	Table:       TestTableLogs,
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
