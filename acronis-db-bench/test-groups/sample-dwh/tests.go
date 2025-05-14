package sample_dwh

import (
	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Sample DWH tests group")

	// ADVM Tasks tests
	tg.Add(&TestInsertAdvmTasks)
	tg.Add(&TestSelectAdvmTasksLast)
	tg.Add(&TestSelectAdvmTasksCodePerWeek)

	// ADVM Resources tests
	tg.Add(&TestInsertAdvmResources)
	tg.Add(&TestInsertAdvmResourcesStatuses)
	tg.Add(&TestInsertAdvmAgentResources)

	// ADVM Agents and Devices tests
	tg.Add(&TestInsertAdvmAgents)
	tg.Add(&TestInsertAdvmDevices)

	// ADVM Backup and Archive tests
	tg.Add(&TestInsertAdvmBackupResources)
	tg.Add(&TestInsertAdvmBackups)
	tg.Add(&TestInsertAdvmArchives)
	tg.Add(&TestInsertAdvmVaults)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

/*
 * Sample DWH simulation tests
 */

// TestInsertAdvmTasks inserts into the 'adv monitoring tasks' table
var TestInsertAdvmTasks = engine.TestDesc{
	Name:        "insert-advmtasks",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring tasks' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmTasks,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestSelectAdvmTasksLast selects last inserted row from the 'adv monitoring tasks' table
var TestSelectAdvmTasksLast = engine.TestDesc{
	Name:        "select-advmtasks-last",
	Metric:      "values/sec",
	Description: "get number of rows grouped by week+result_code",
	Category:    engine.TestSelect,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmTasks,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		var idToRead int64

		var where = func(worker *benchmark.BenchmarkWorker) map[string][]string { //nolint:revive
			return map[string][]string{
				"origin": {"1", "2", "3"},
			}
		}

		var orderBy = func(worker *benchmark.BenchmarkWorker) []string { //nolint:revive
			return []string{"asc(id)"}
		}
		engine.TestSelectRun(b, testDesc, nil, []string{"id"}, []interface{}{&idToRead}, where, orderBy, 1)
	},
}

// TestSelectAdvmTasksCodePerWeek selects number of rows grouped by week+result_code
var TestSelectAdvmTasksCodePerWeek = engine.TestDesc{
	Name:        "select-advmtasks-codeperweek",
	Metric:      "values/sec",
	Description: "get number of rows grouped by week+result_code",
	Category:    engine.TestSelect,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmTasks,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		// need to implement it
		b.Exit("%s: is not implemented!\n", testDesc.Name)
	},
}

// TestInsertAdvmResources inserts into the 'adv monitoring resources' table
var TestInsertAdvmResources = engine.TestDesc{
	Name:        "insert-advmresources",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring resources' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmResources,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmResourcesStatuses inserts into the 'adv monitoring resources statuses' table
var TestInsertAdvmResourcesStatuses = engine.TestDesc{
	Name:        "insert-advmresourcesstatuses",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring resources statuses' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmResourcesStatuses,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmAgentResources inserts into the 'adv monitoring agent resources' table
var TestInsertAdvmAgentResources = engine.TestDesc{
	Name:        "insert-advmagentresources",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring agent resources' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmAgentsResources,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmAgents inserts into the 'adv monitoring agents' table
var TestInsertAdvmAgents = engine.TestDesc{
	Name:        "insert-advmagents",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring agents' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmAgents,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmBackupResources inserts into the 'adv monitoring backup resources' table
var TestInsertAdvmBackupResources = engine.TestDesc{
	Name:        "insert-advmbackupresources",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring backup resources' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmBackupResources,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmBackups inserts into the 'adv monitoring backups' table
var TestInsertAdvmBackups = engine.TestDesc{
	Name:        "insert-advmbackups",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring backups' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmBackups,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmArchives inserts into the 'adv monitoring archives' table
var TestInsertAdvmArchives = engine.TestDesc{
	Name:        "insert-advmarchives",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring archives' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmArchives,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmVaults inserts into the 'adv monitoring vaults' table
var TestInsertAdvmVaults = engine.TestDesc{
	Name:        "insert-advmvaults",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring vaults' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmVaults,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}

// TestInsertAdvmDevices inserts into the 'adv monitoring devices' table
var TestInsertAdvmDevices = engine.TestDesc{
	Name:        "insert-advmdevices",
	Metric:      "rows/sec",
	Description: "insert into the 'adv monitoring devices' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	Databases:   []db.DialectName{db.POSTGRES, db.MSSQL},
	Table:       TestTableAdvmDevices,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestInsertGeneric(b, testDesc)
	},
}
