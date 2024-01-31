package main

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/acronis/perfkit/benchmark"
)

/*
 * Helpers
 */

func createTables(b *benchmark.Benchmark) {
	dbOpts := b.TestOpts.(*TestOpts).DBOpts
	usedTables := benchmark.NewSet()

	_, tests := GetTests()
	for _, t := range tests {
		if t.table.TableName != "" && t.dbIsSupported(dbOpts.Driver) {
			usedTables.Add(t.table.TableName)
		}
	}

	fmt.Printf("creating the tables ... ")

	c := dbConnector(b)
	for _, tableDesc := range TestTables {
		if usedTables.Contains(tableDesc.TableName) {
			tableDesc.Create(c, b)
		}
	}
	b.TenantsCache.CreateTables(c)
	c.CreateSequence(benchmark.SequenceName)
	c.Release()

	eb := NewEventBus(&dbOpts, b.Logger)
	eb.CreateTables()

	fmt.Printf("done\n")
}

func dbConnector(b *benchmark.Benchmark) *benchmark.DBConnector {
	return benchmark.NewDBConnector(&b.TestOpts.(*TestOpts).DBOpts, 0, b.Logger, 1)
}

func cleanupTables(b *benchmark.Benchmark) {
	dbOpts := b.TestOpts.(*TestOpts).DBOpts

	if dbOpts.DontCleanup {
		fmt.Printf("skip acronis_db_bench_* tables cleanup\n")

		return
	}

	fmt.Printf("cleaning up the test tables ... ")

	c := dbConnector(b)

	for tableName := range TestTables {
		c.DropTable(tableName)
	}

	b.TenantsCache.DropTables(c)
	c.DropSequence(benchmark.SequenceName)
	c.Release()

	eb := NewEventBus(&b.TestOpts.(*TestOpts).DBOpts, b.Logger)
	eb.DropTables()

	fmt.Printf("done\n")
}

func getDBInfo(b *benchmark.Benchmark, content []string) (ret string) {
	c := dbConnector(b)

	tableNames := make([]string, 0, len(TestTables))
	for k := range TestTables {
		tableNames = append(tableNames, k)
	}
	sort.Strings(tableNames)

	ret += "\n"
	ret += fmt.Sprintf("DATABASE INFO:\n\n%s\n", strings.Join(content, "\n"))
	ret += "\nSCHEMAS / INDEXES INFO:\n\n"
	ret += strings.Join(c.GetTablesSchemaInfo(tableNames), "\n")
	ret += "\nTABLES INFO:\n\n"
	ret += strings.Join(c.GetTablesVolumeInfo(tableNames), "\n")
	ret += "\n\n"

	c.Release()

	return ret
}

func formatSQL(sqlTemlate, driver string) string {
	if driver == benchmark.POSTGRES {
		return sqlTemlate
	}

	var re = regexp.MustCompile(`\$\d+`)

	return re.ReplaceAllString(sqlTemlate, "?")
}
