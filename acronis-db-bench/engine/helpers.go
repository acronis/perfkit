package engine

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	dataset "github.com/acronis/perfkit/acronis-db-bench/dataset-source"
	events "github.com/acronis/perfkit/acronis-db-bench/event-bus"
	tenants "github.com/acronis/perfkit/acronis-db-bench/tenants-cache"
)

const SequenceName = "acronis_db_bench_sequence" // SequenceName is the name of the sequence used for generating IDs

// FatalError prints error message and exits with code 127
func FatalError(err string) {
	fmt.Printf("fatal error: %v", err)
	os.Exit(127)
}

/*
 * Helpers
 */

func createTables(b *benchmark.Benchmark) {
	dbOpts := b.TestOpts.(*TestOpts).DBOpts
	usedTables := benchmark.NewSet()

	var dialectName, err = db.GetDialectName(dbOpts.ConnString)
	if err != nil {
		b.Exit(err)
	}

	_, tests := GetTests()
	for _, t := range tests {
		if t.Table.TableName != "" && t.dbIsSupported(dialectName) {
			usedTables.Add(t.Table.TableName)
		}
	}

	fmt.Printf("creating the tables ... ")

	c := DbConnector(b)
	for _, tableDesc := range testRegistry.GetTables() {
		if usedTables.Contains(tableDesc.TableName) && tableDesc.dbIsSupported(dialectName) {
			tableDesc.Create(c, b)
		}
	}

	var tc = b.Vault.(*DBTestData).TenantsCache
	if tc == nil {
		tc = tenants.NewTenantsCache(b)
		b.Vault.(*DBTestData).TenantsCache = tc
	}
	tc.CreateTables(c.Database)
	c.Database.CreateSequence(SequenceName)
	c.Release()

	eb := events.NewEventBus(c.Database, b.Logger)
	eb.CreateTables()

	fmt.Printf("done\n")
}

func DbConnector(b *benchmark.Benchmark) *DBConnector {
	var conn, err = NewDBConnector(&b.TestOpts.(*TestOpts).DBOpts, -1, b.Logger, 1)
	if err != nil {
		FatalError(err.Error())
	}

	return conn
}

func CleanupTables(b *benchmark.Benchmark) {
	dbOpts := b.TestOpts.(*TestOpts).DBOpts

	if dbOpts.DontCleanup {
		fmt.Printf("skip acronis_db_bench_* tables cleanup\n")

		return
	}

	fmt.Printf("cleaning up the test tables ... ")

	var c = DbConnector(b)

	for tableName := range testRegistry.GetTables() {
		if err := c.Database.DropTable(tableName); err != nil {
			b.Exit("failed drop table: %s", err)
		}
	}

	if b.Vault.(*DBTestData).TenantsCache == nil {
		b.Vault.(*DBTestData).TenantsCache = tenants.NewTenantsCache(b)
	}

	if err := b.Vault.(*DBTestData).TenantsCache.DropTables(c.Database); err != nil {
		b.Exit("failed drop table: %s", err)
	}

	if err := c.Database.DropSequence(SequenceName); err != nil {
		b.Exit("failed drop sequence: %s", err)
	}

	if b.Vault.(*DBTestData).EventBus == nil {
		b.Vault.(*DBTestData).EventBus = events.NewEventBus(c.Database, b.Logger)
	}

	if err := b.Vault.(*DBTestData).EventBus.DropTables(); err != nil {
		b.Exit("failed drop table: %s", err)
	}

	c.Release()

	fmt.Printf("done\n")
}

func getDBInfo(b *benchmark.Benchmark, content []string) (ret string) {
	c := DbConnector(b)

	tableNames := make([]string, 0, len(testRegistry.GetTables()))
	for k := range testRegistry.GetTables() {
		tableNames = append(tableNames, k)
	}
	sort.Strings(tableNames)

	var tableSchemaInfo, tablesVolumeInfo []string
	var err error

	tableSchemaInfo, err = c.Database.GetTablesSchemaInfo(tableNames)
	if err != nil {
		FatalError(err.Error())
	}

	tablesVolumeInfo, err = c.Database.GetTablesVolumeInfo(tableNames)
	if err != nil {
		FatalError(err.Error())
	}

	ret += "\n"
	ret += fmt.Sprintf("DATABASE INFO:\n\n%s\n", strings.Join(content, "\n"))
	ret += "\nSCHEMAS / INDEXES INFO:\n\n"
	ret += strings.Join(tableSchemaInfo, "\n")
	ret += "\nTABLES INFO:\n\n"
	ret += strings.Join(tablesVolumeInfo, "\n")
	ret += "\n\n"

	c.Release()

	return ret
}

func FormatSQL(sqlTemlate string, dialectName db.DialectName) string {
	if dialectName == db.POSTGRES {
		return sqlTemlate
	}

	var re = regexp.MustCompile(`\$\d+`)

	return re.ReplaceAllString(sqlTemlate, "?")
}

// NewParquetFileDataSourceForRandomizer creates a new parquet DataSetSource instance and register as plugin for Randomizer
func NewParquetFileDataSourceForRandomizer(bench *benchmark.Benchmark, filePath string) error {
	if bench.Randomizer == nil {
		bench.Randomizer = benchmark.NewRandomizer(bench.CommonOpts.RandSeed, bench.CommonOpts.Workers)
	}

	var source, err = dataset.NewParquetFileDataSource(filePath)
	if err != nil {
		return err
	}

	var columns = source.GetColumnNames()

	var registeredColumns []string
	for _, column := range columns {
		registeredColumns = append(registeredColumns, fmt.Sprintf("dataset.%s", column))
	}

	var dataSourcePlugin = &DataSetSourcePlugin{
		source:  source,
		columns: registeredColumns,
	}

	bench.Randomizer.RegisterPlugin("dataset", dataSourcePlugin)
	for _, worker := range bench.Workers {
		worker.Randomizer.RegisterPlugin("dataset", dataSourcePlugin)
	}

	return nil
}

type DataSetSourcePlugin struct {
	source dataset.DataSetSource

	columns       []string
	currentValues map[string]interface{}
}

func (p *DataSetSourcePlugin) GenCommonFakeValue(columnType string, rz *benchmark.Randomizer, cardinality int) (bool, interface{}) {
	if len(p.columns) == 0 {
		return false, nil
	}

	// Check only first column for reading next row
	if columnType != p.columns[0] {
		return false, nil
	}

	var row, err = p.source.GetNextRow()
	if err != nil {
		return false, nil
	}

	if row == nil {
		return false, nil
	}

	p.currentValues = make(map[string]interface{}, len(row))
	for i, value := range row {
		p.currentValues[p.columns[i]] = value
	}

	return true, p.currentValues[columnType]
}

func (p *DataSetSourcePlugin) GenFakeValue(columnType string, rz *benchmark.Randomizer, cardinality int, preGenerated map[string]interface{}) (bool, interface{}) {
	if len(p.columns) == 0 {
		return false, nil
	}

	var value, ok = p.currentValues[columnType]
	if !ok {
		return false, nil
	}

	if columnType == "dataset.Date" {
		if valueStr, casted := value.(string); casted {
			var date, err = db.ParseTimeUTC(valueStr)
			if err != nil {
				return false, nil
			}

			return true, date
		}
	}

	return true, value
}
