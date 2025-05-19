package basic_scenarios

import (
	"github.com/acronis/perfkit/acronis-db-bench/engine"
	"github.com/acronis/perfkit/benchmark"
)

func (suite *TestingSuite) TestMediumTableTests() {
	var mediumTableTestSuite = engine.NewPerfSuite().

		/* Prepare tenants and cti entities */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 100
		}).
		ScheduleTest("insert-tenant").

		/* Insert */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 5
		}).
		ScheduleTest("insert-medium").

		/* Update */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 5
		}).
		ScheduleTest("update-medium").

		/* Select */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 5
		}).
		ScheduleTest("select-medium-rand")

	var mediumTestOpts = &engine.TestOpts{
		DBOpts: engine.DatabaseOpts{ConnString: suite.ConnString},
	}

	var b = benchmark.NewBenchmark()
	b.TestOpts = mediumTestOpts

	b.AddOpts = func() benchmark.TestOpts {
		var testOpts = engine.TestOpts{
			DBOpts:    engine.DatabaseOpts{ConnString: suite.ConnString},
			BenchOpts: engine.BenchOpts{TenantsWorkingSet: 50},
		}

		b.Cli.AddFlagGroup("Database options", "", &testOpts.DBOpts)
		b.Cli.AddFlagGroup("acronis-db-bench specific options", "", &testOpts.BenchOpts)
		b.Cli.AddFlagGroup("Testcase specific options", "", &testOpts.TestcaseOpts)
		b.Cli.AddFlagGroup("CTI-pattern simulation test options", "", &testOpts.CTIOpts)

		return mediumTestOpts
	}

	b.InitOpts()

	d := engine.DBTestData{
		EffectiveBatch: 1,
	}
	b.Vault = &d

	d.Scores = make(map[string][]benchmark.Score)

	for _, s := range engine.TestCategories {
		d.Scores[s] = []benchmark.Score{}
	}

	mediumTableTestSuite.Execute(b, nil, 1)

	// Clear tables
	engine.CleanupTables(b)
}
