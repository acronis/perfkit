// Package main provides benchmarking utilities for databases.
// It includes various tests and options for database performance analysis.
package main

import (
	"fmt"
	"net/http"
	"runtime"
	"sort"
	"strings"

	_ "net/http/pprof"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	_ "github.com/acronis/perfkit/db/es"  // es drivers
	_ "github.com/acronis/perfkit/db/sql" // sql drivers

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
	CTIsWorkingSet    int    `long:"ctis-working-set" description:"set CTI working set" required:"false" default:"1000"`
	ProfilerPort      int    `long:"profiler-port" description:"open profiler on given port (e.g. 6060)" required:"false" default:"0"`
	Describe          bool   `long:"describe" description:"describe what test is going to do" required:"false"`
	DescribeAll       bool   `long:"describe-all" description:"describe all the tests" required:"false"`
	Explain           bool   `long:"explain" description:"prepend the test queries by EXPLAIN ANALYZE" required:"false"`
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

// DBWorkerData is a structure to store all the worker data
type DBWorkerData struct {
	workingConn  *DBConnector
	tenantsCache *DBConnector
}

var header = strings.Repeat("=", 120) + "\n"

// Main is the main function of the acronis-db-bench
func Main() {
	fmt.Printf(header) //nolint:staticcheck

	printVersion()

	b := benchmark.New()

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

		if b.TestOpts.(*TestOpts).BenchOpts.Explain {
			return
		}

		if strings.TrimSpace(b.TestOpts.(*TestOpts).BenchOpts.Test) == TestBaseAll.name {
			format = "test: %-40s; rows-before-test: %8d; time: %5.1f sec; workers: %2d; loops: %8d; batch: %4d; rate: %8s %s;\n"
		} else {
			format = "test: %s; rows-before-test: %d; time: %.1f sec; workers: %d; loops: %d; batch: %4d; rate: %s %s\n"
		}

		fmt.Printf(format, testData.TestDesc.name, testData.TestDesc.table.RowsCount, score.Seconds, score.Workers, score.Loops,
			b.Vault.(*DBTestData).EffectiveBatch, score.FormatRate(4), score.Metric)
	}

	b.InitOpts()

	testOpts, ok := b.TestOpts.(*TestOpts)
	if !ok {
		b.Exit("db type conversion error")
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

			str := fmt.Sprintf("  -- %s", g.name) //nolint:perfsprint
			fmt.Printf("\n%s %s\n\n", str, strings.Repeat("-", 130-len(str)))

			var testsOutput []string
			for _, t := range g.tests {
				if dialectName != "" && !t.dbIsSupported(dialectName) {
					continue
				}
				testsOutput = append(testsOutput, fmt.Sprintf("  %-39s : %s : %s\n", t.name, t.getDBs(), t.description))
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
		b.PreWorker = func(workerId int) {
			var workerData = b.WorkerData[workerId].(*DBWorkerData)

			if workerData.workingConn != nil {
				workerData.workingConn.database.Close()
			}

			if workerData.tenantsCache != nil {
				workerData.tenantsCache.database.Close()
			}
		}
	}

	c := dbConnector(b)

	driver, version, err := c.database.GetVersion()
	if err != nil {
		b.Exit("Failed to get database version: %v", err)
	}

	fmt.Printf("Connected to '%s' database: %s\n", driver, version)
	fmt.Printf(header) //nolint:staticcheck

	content, dbInfo, err := c.database.GetInfo(version)
	if err != nil {
		b.Exit("Failed to get database info: %v", err)
	}

	if testOpts.BenchOpts.Info || b.Logger.LogLevel > benchmark.LogInfo {
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

		if b.Logger.LogLevel > benchmark.LogInfo && !testOpts.BenchOpts.Info {
			b.Log(benchmark.LogTrace, 0, getDBInfo(b, content))
		}

		if b.TestOpts.(*TestOpts).BenchOpts.Events {
			var workingConn *DBConnector
			if workingConn, err = NewDBConnector(&b.TestOpts.(*TestOpts).DBOpts, -1, b.Logger, 1); err != nil {
				return
			}

			b.Vault.(*DBTestData).EventBus = events.NewEventBus(workingConn.database, b.Logger)
			b.Vault.(*DBTestData).EventBus.CreateTables()
		}
	}

	b.Finish = func() {
		if b.TestOpts.(*TestOpts).BenchOpts.Events {
			b.Vault.(*DBTestData).EventBus.Stop()
		}
	}

	if testOpts.DBOpts.Reconnect {
		switch runtime.GOOS {
		case "darwin", "linux":
			val, err := benchmark.GetSysctlValueInt("net.inet.tcp.msl")
			if err == nil {
				if val > int64(1) {
					b.Log(benchmark.LogWarn, 0, "The --reconnect test requires low TCP TIME_WAIT delay (e.g. 1 msec), "+
						fmt.Sprintf("current value is %d msec, do `sysctl -w net.inet.tcp.msl=1`", val))
				}
			}

			val, err = benchmark.GetSysctlValueInt("kern.ipc.somaxconn")
			if err == nil {
				required := b.CommonOpts.Workers * 2
				if val < int64(required) {
					b.Log(benchmark.LogWarn, 0, fmt.Sprintf("The --reconnect test requires `kern.ipc.somaxconn` to be at least %d, ", required)+
						fmt.Sprintf("current value is %d msec, do `sysctl -w kern.ipc.somaxconn=%d`", val, required))
				}
			}
		default:
			b.Log(benchmark.LogWarn, 0, "Reconnect test is not supported on this platform: %s", runtime.GOOS)
		}

		c.DbOpts.MaxOpenConns = 1
	}

	if testOpts.BenchOpts.Query != "" {
		TestRawQuery.launcherFunc(b, &TestRawQuery)
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
	test.launcherFunc(b, test)
}

func describeOne(b *benchmark.Benchmark, testDesc *TestDesc) {
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 1
	if b.Logger.LogLevel <= benchmark.LogInfo {
		b.Logger.LogLevel = benchmark.LogInfo
	}

	fmt.Printf("\n")
	fmt.Printf(header) //nolint:staticcheck
	fmt.Printf("Test:        %s\n", testDesc.name)
	fmt.Printf("Metric:      %s\n", testDesc.metric)
	fmt.Printf("Description: %s\n", testDesc.description)
	fmt.Printf(header) //nolint:staticcheck

	if testDesc.name == TestBaseAll.name {
		fmt.Print("describe: run all the tests in a loop\n")
	} else {
		testDesc.launcherFunc(b, testDesc)
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
