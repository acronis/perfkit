// Package engine provides benchmarking utilities for databases.
// It includes various tests and options for database performance analysis.
package engine

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"runtime"
	"sort"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"

	events "github.com/acronis/perfkit/acronis-db-bench/event-bus"
	tenants "github.com/acronis/perfkit/acronis-db-bench/tenants-cache"
)

// Version is a version of the acronis-db-bench
var Version = "1-main-dev"

func printVersion() {
	fmt.Printf("Acronis Database Benchmark: version v%s\n", Version)
}

/*
 * TODO:
 * - to add and 'insert' test with UUIDv4 (random) and UUIDv7 (ordered) as PK
 * - make some basic checks about memory buffers allocation and show a warning it doesn't seem reasonable
 * - check current node load to show a warning if machine is overloaded
 */

// TestOpts is a structure to store all the test options
type TestOpts struct {
	DBOpts       DatabaseOpts
	BenchOpts    BenchOpts
	TestcaseOpts TestcaseOpts
	CTIOpts      CTIOpts
}

// BenchOpts is a structure to store all the benchmark options
type BenchOpts struct {
	Batch             int    `short:"b" long:"batch" description:"batch sets the amount of rows per transaction" required:"false" default:"0"`
	Test              string `short:"t" long:"test" description:"select a test to execute, run --list to see available tests list" required:"false"`
	List              bool   `short:"a" long:"list" description:"list available tests" required:"false"`
	Cleanup           bool   `short:"C" long:"cleanup" description:"delete/truncate all test DB tables and exit"`
	Init              bool   `short:"I" long:"init" description:"create all test DB tables and exit" `
	Chunk             int    `short:"u" long:"chunk" description:"chunk size for 'all' test" required:"false" default:"500000"`
	Limit             int    `short:"U" long:"limit" description:"total rows limit for 'all' test" required:"false" default:"2000000"`
	Info              bool   `short:"i" long:"info" description:"provide information about tables & indexes" required:"false"`
	Events            bool   `short:"e" long:"events" description:"simulate event generation for every new object" required:"false"`
	TenantsWorkingSet int    `long:"tenants-working-set" description:"set tenants working set" required:"false" default:"10000"`
	TenantConnString  string `long:"tenants-storage-connection-string" description:"connection string for tenant storage" required:"false"`
	ParquetDataSource string `long:"parquet-data-source" description:"path to the parquet file" required:"false"`
	CTIsWorkingSet    int    `long:"ctis-working-set" description:"set CTI working set" required:"false" default:"1000"`
	ProfilerPort      int    `long:"profiler-port" description:"open profiler on given port (e.g. 6060)" required:"false" default:"0"`
	Describe          bool   `long:"describe" description:"describe what test is going to do" required:"false"`
	DescribeAll       bool   `long:"describe-all" description:"describe all the tests" required:"false"`
	Query             string `short:"q" long:"query" description:"execute given query, one can use:\n{CTI} - for random CTI UUID\n{TENANT} - randon tenant UUID"`
}

// CTIOpts is a structure to store all the CTI options
type CTIOpts struct {
	CTICardinality int `short:"D" long:"cti-cardinality" description:"CTI values cardinality, i.e. variety of unique numbers" required:"false" default:"1000"`
}

// TestcaseOpts is a structure to store all the test case options
type TestcaseOpts struct {
	MinBlobSize int `long:"min-blob-size" description:"defines min blob size for the 'insert-blob' test (default 0)" required:"false" default:"0"`
	MaxBlobSize int `long:"max-blob-size" description:"defines max blob size for the 'insert-blob' test (default 52428800)" required:"false" default:"52428800"`
}

// DBTestData is a structure to store all the test data
type DBTestData struct {
	TestDesc       *TestDesc
	EventBus       *events.EventBus
	TenantsCache   *tenants.TenantsCache
	EffectiveBatch int // EffectiveBatch reflects the default value if the --batch option is not set, it can be different for different tests

	scores map[string][]benchmark.Score
}

var header = strings.Repeat("=", 120) + "\n"

// Main is the main function of the acronis-db-bench
func Main() {
	fmt.Printf(header) //nolint:staticcheck

	printVersion()

	b := benchmark.NewBenchmark()

	b.AddOpts = func() benchmark.TestOpts {
		var testOpts TestOpts
		b.Cli.AddFlagGroup("Database options", "", &testOpts.DBOpts)
		b.Cli.AddFlagGroup("acronis-db-bench specific options", "", &testOpts.BenchOpts)
		b.Cli.AddFlagGroup("Testcase specific options", "", &testOpts.TestcaseOpts)
		b.Cli.AddFlagGroup("CTI-pattern simulation test options", "", &testOpts.CTIOpts)

		return &testOpts
	}

	b.PrintScore = func(score benchmark.Score) {
		testData := b.Vault.(*DBTestData)
		var format string

		if b.TestOpts.(*TestOpts).DBOpts.Explain {
			return
		}

		if strings.TrimSpace(b.TestOpts.(*TestOpts).BenchOpts.Test) == TestBaseAll.Name {
			format = "test: %-40s; rows-before-test: %8d; time: %5.1f sec; workers: %2d; loops: %8d; batch: %4d; rate: %8s %s;\n"
		} else {
			format = "test: %s; rows-before-test: %d; time: %.1f sec; workers: %d; loops: %d; batch: %4d; rate: %s %s\n"
		}

		fmt.Printf(format, testData.TestDesc.Name, testData.TestDesc.Table.RowsCount, score.Seconds, score.Workers, score.Loops,
			b.Vault.(*DBTestData).EffectiveBatch, score.FormatRate(4), score.Metric)
	}

	b.InitOpts()

	testOpts, ok := b.TestOpts.(*TestOpts)
	if !ok {
		b.Exit("db type conversion error")
	}

	var connStringErr error
	if testOpts.DBOpts.ConnString, connStringErr = constructConnStringFromOpts(testOpts.DBOpts.ConnString, testOpts.DBOpts.Driver, testOpts.DBOpts.Dsn); connStringErr != nil {
		b.Exit("failed to construct connection string: %v", connStringErr)
	}

	d := DBTestData{}
	b.Vault = &d

	d.scores = make(map[string][]benchmark.Score)

	for _, s := range TestCategories {
		d.scores[s] = []benchmark.Score{}
	}

	if b.TestOpts.(*TestOpts).BenchOpts.Batch > 0 {
		b.Vault.(*DBTestData).EffectiveBatch = b.TestOpts.(*TestOpts).BenchOpts.Batch
	} else {
		b.Vault.(*DBTestData).EffectiveBatch = 1
	}

	var dialectName, err = db.GetDialectName(b.TestOpts.(*TestOpts).DBOpts.ConnString)
	if err != nil {
		b.Exit("failed to get dialect name: %v", err)
	}

	if testOpts.BenchOpts.List {
		groups, _ := GetTests()
		fmt.Printf(header) //nolint:staticcheck
		for _, g := range groups {

			str := fmt.Sprintf("  -- %s", g.Name) //nolint:perfsprint
			fmt.Printf("\n%s %s\n\n", str, strings.Repeat("-", 130-len(str)))

			var testsOutput []string
			for _, t := range g.tests {
				if dialectName != "" && !t.dbIsSupported(dialectName) {
					continue
				}
				testsOutput = append(testsOutput, fmt.Sprintf("  %-39s : %s : %s\n", t.Name, t.getDBs(), t.Description))
			}
			sort.Strings(testsOutput)
			fmt.Print(strings.Join(testsOutput, ""))
		}
		fmt.Printf("\n")
		fmt.Printf("Databases symbol legend:\n\n ")
		for _, db := range db.GetDatabases() {
			fmt.Printf(" %s - %s;", db.Symbol, db.Name)
		}
		fmt.Printf("\n\n")
		b.Exit()
	}

	if testOpts.BenchOpts.Describe {
		describeTest(b, testOpts)
		b.Exit()
	}

	if testOpts.BenchOpts.DescribeAll {
		describeAllTests(b, testOpts)
		b.Exit()
	}

	if testOpts.BenchOpts.Cleanup {
		cleanupTables(b)
		b.Exit()
	}

	if testOpts.BenchOpts.Init {
		createTables(b)
		b.Exit()
	}

	if testOpts.DBOpts.Reconnect {
		b.WorkerPreRunFunc = func(worker *benchmark.BenchmarkWorker) {
			var workerData = worker.Data.(*DBWorkerData)

			if workerData.workingConn != nil {
				workerData.workingConn.Close()
			}

			if workerData.tenantsCache != nil {
				workerData.tenantsCache.Close()
			}

			worker.Data = nil
		}
	}

	c := DbConnector(b)

	driver, version, err := c.Database.GetVersion()
	if err != nil {
		b.Exit("Failed to get database version: %v", err)
	}

	fmt.Printf("Connected to '%s' database: %s\n", driver, version)
	fmt.Printf(header) //nolint:staticcheck

	content, dbInfo, err := c.Database.GetInfo(version)
	if err != nil {
		b.Exit("Failed to get database info: %v", err)
	}

	// Has to be returned back to connection pool because it is not used anywhere else
	c.Release()

	if testOpts.BenchOpts.Info || b.Logger.GetLevel() > logger.LevelInfo {
		if testOpts.BenchOpts.Info {
			fmt.Printf(getDBInfo(b, content)) //nolint:staticcheck
		}
	}

	if !b.CommonOpts.Quiet {
		if dbInfo != nil {
			dbInfo.ShowRecommendations()
		}
	}

	if testOpts.BenchOpts.ProfilerPort > 0 {
		go func() {
			err = http.ListenAndServe(fmt.Sprintf("localhost:%d", testOpts.BenchOpts.ProfilerPort), nil)
			if err != nil {
				b.Exit("Failed to start profiler server: %v", err)
			}
		}()
		fmt.Printf("running profiler endpoint @ http://localhost:%d/debug/pprof/\n", testOpts.BenchOpts.ProfilerPort)
		fmt.Printf("to collect the profiler log run: go tool pprof 'http://localhost:%d/debug/pprof/profile?seconds=10'\n", testOpts.BenchOpts.ProfilerPort)
	}

	b.Init = func() {
		b.Vault.(*DBTestData).TenantsCache = tenants.NewTenantsCache(b)

		b.Vault.(*DBTestData).TenantsCache.SetTenantsWorkingSet(b.TestOpts.(*TestOpts).BenchOpts.TenantsWorkingSet)
		b.Vault.(*DBTestData).TenantsCache.SetCTIsWorkingSet(b.TestOpts.(*TestOpts).BenchOpts.CTIsWorkingSet)

		if b.Logger.GetLevel() > logger.LevelInfo && !testOpts.BenchOpts.Info {
			b.Log(logger.LevelTrace, 0, getDBInfo(b, content))
		}

		if b.TestOpts.(*TestOpts).BenchOpts.Events {
			var workingConn *DBConnector
			if workingConn, err = NewDBConnector(&b.TestOpts.(*TestOpts).DBOpts, -1, b.Logger, 1); err != nil {
				return
			}

			b.Vault.(*DBTestData).EventBus = events.NewEventBus(workingConn.Database, b.Logger)
			b.Vault.(*DBTestData).EventBus.CreateTables()
		}

		if b.TestOpts.(*TestOpts).BenchOpts.ParquetDataSource != "" {
			if err = NewParquetFileDataSourceForRandomizer(b, b.TestOpts.(*TestOpts).BenchOpts.ParquetDataSource); err != nil {
				b.Exit("failed to create parquet data source: %v", err)
			}
		}
	}

	b.Finish = func() {
		b.Logger.Debug("finishing")
		if b.TestOpts.(*TestOpts).BenchOpts.Events {
			b.Vault.(*DBTestData).EventBus.Stop()
		}
	}

	if testOpts.DBOpts.Reconnect {
		switch runtime.GOOS {
		case "darwin":
			val, err := benchmark.GetSysctlValueInt("net.inet.tcp.msl")
			if err == nil {
				if val > int64(1) {
					b.Logger.Warn("The --reconnect test requires low TCP TIME_WAIT delay (e.g. 1 msec), " +
						fmt.Sprintf("current value is %d msec, do `sysctl -w net.inet.tcp.msl=1`", val))
				}
			}

			val, err = benchmark.GetSysctlValueInt("kern.ipc.somaxconn")
			if err == nil {
				required := b.CommonOpts.Workers * 2
				if val < int64(required) {
					b.Logger.Warn("The --reconnect test requires `kern.ipc.somaxconn` to be at least %d, " +
						fmt.Sprintf("current value is %d msec, do `sysctl -w kern.ipc.somaxconn=%d`", val, required))
				}
			}
		case "linux":
			val, err := benchmark.GetSysctlValueInt("net.ipv4.tcp_fin_timeout")
			if err == nil {
				if val > int64(1) {
					b.Logger.Warn("The --reconnect test requires low TCP TIME_WAIT delay (e.g. 1 sec), " +
						fmt.Sprintf("current value is %d sec, do `sysctl -w net.ipv4.tcp_fin_timeout=1`", val))
				}
			}

			val, err = benchmark.GetSysctlValueInt("net.core.somaxconn")
			if err == nil {
				required := b.CommonOpts.Workers * 2
				if val < int64(required) {
					b.Logger.Warn("The --reconnect test requires `net.core.somaxconn` to be at least %d, " +
						fmt.Sprintf("current value is %d msec, do `sysctl -w net.core.somaxconn=%d`", val, required))
				}
			}
		default:
			b.Logger.Warn("Reconnect test is not supported on this platform: %s", runtime.GOOS)
		}

		c.DbOpts.MaxOpenConns = 1
	}

	if testOpts.BenchOpts.Query != "" {
		TestRawQuery.LauncherFunc(b, &TestRawQuery)
	} else if testOpts.BenchOpts.Test != "" {
		executeTests(b, testOpts)
	} else if !testOpts.BenchOpts.Info {
		b.Exit("either --test or --info options must be set\n")
	}

	b.Exit()
}

func main() {
	Main()
}

func executeTests(b *benchmark.Benchmark, testOpts *TestOpts) {
	_, tests := GetTests()
	_, exists := tests[testOpts.BenchOpts.Test]
	if !exists {
		b.Exit(fmt.Sprintf("Test: '%s' doesn't exist, see the list of available tests using --list option\n", testOpts.BenchOpts.Test))
	}

	var dialectName, err = db.GetDialectName(testOpts.DBOpts.ConnString)
	if err != nil {
		b.Exit(err)
	}

	test := tests[testOpts.BenchOpts.Test]
	if !test.dbIsSupported(dialectName) {
		b.Exit(fmt.Sprintf("Test: '%s' doesn't support '%s' database\n", testOpts.BenchOpts.Test, dialectName))
	}
	test.LauncherFunc(b, test)
}

func describeOne(b *benchmark.Benchmark, testDesc *TestDesc) {
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 1
	if b.Logger.GetLevel() <= logger.LevelInfo {
		b.Logger.SetLevel(logger.LevelInfo)
	}

	fmt.Printf("\n")
	fmt.Printf(header) //nolint:staticcheck
	fmt.Printf("Test:        %s\n", testDesc.Name)
	fmt.Printf("Metric:      %s\n", testDesc.Metric)
	fmt.Printf("Description: %s\n", testDesc.Description)
	fmt.Printf(header) //nolint:staticcheck

	if testDesc.Name == TestBaseAll.Name {
		fmt.Print("describe: run all the tests in a loop\n")
	} else {
		testDesc.LauncherFunc(b, testDesc)
	}
	fmt.Printf("\n")
}

func describeTest(b *benchmark.Benchmark, testOpts *TestOpts) {
	_, tests := GetTests()
	_, exists := tests[testOpts.BenchOpts.Test]
	if !exists {
		b.Exit(fmt.Sprintf("Test: '%s' doesn' exist, see the list of available tests using --list option\n", testOpts.BenchOpts.Test))
	}
	test := tests[testOpts.BenchOpts.Test]
	describeOne(b, test)
}

func describeAllTests(b *benchmark.Benchmark, _ *TestOpts) {
	_, tests := GetTests()
	for _, t := range tests {
		describeOne(b, t)
	}
}
