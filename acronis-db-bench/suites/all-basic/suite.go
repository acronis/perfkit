package all_basic

import (
	"github.com/acronis/perfkit/benchmark"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var perfSuiteAllBasic = engine.NewPerfSuite().
		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 10
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 0
		}).
		ScheduleTest("select-one").

		/* Prepare tenants and cti entities */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 10000
		}).
		ScheduleTest("insert-tenant").
		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 1000
		}).
		ScheduleTest("insert-cti").

		/* Insert */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 5
		}).
		ScheduleTest("insert-light").
		ScheduleTest("insert-medium").
		ScheduleTest("insert-heavy").
		ScheduleTest("insert-json").
		ScheduleTest("insert-ts-sql").
		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = workers
			b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 95
		}).
		ScheduleTest("insert-light").
		ScheduleTest("insert-medium").
		ScheduleTest("insert-json").
		ScheduleTest("insert-ts-sql").

		/* Update */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 2
		}).
		ScheduleTest("update-medium").
		ScheduleTest("update-heavy").
		ScheduleTest("update-heavy-partial-same-val").
		ScheduleTest("update-heavy-same-val").
		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 0
			b.CommonOpts.Workers = workers
			b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 28
		}).
		ScheduleTest("update-medium").
		ScheduleTest("update-heavy").
		ScheduleTest("update-heavy-partial-same-val").
		ScheduleTest("update-heavy-same-val").

		/* Select */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 10
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 0
		}).
		ScheduleTest("select-medium-rand").
		ScheduleTest("select-heavy-rand").
		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 10
			b.CommonOpts.Workers = workers
			b.CommonOpts.Loops = 0
		}).
		ScheduleTest("select-medium-rand").
		ScheduleTest("select-heavy-rand").
		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 10
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 0
		}).
		ScheduleTest("select-medium-last").
		ScheduleTest("select-heavy-last").
		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 10
			b.CommonOpts.Workers = workers
			b.CommonOpts.Loops = 0
		}).
		ScheduleTest("select-medium-last").
		ScheduleTest("select-heavy-last").

		/* Other select's */

		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 10
			b.CommonOpts.Workers = 1
			b.CommonOpts.Loops = 0
		}).
		ScheduleTest("select-heavy-last-tenant").
		ScheduleTest("select-heavy-rand-tenant-like").
		ScheduleTest("select-heavy-last-tenant-cti").
		ScheduleTest("select-json-by-indexed-value").
		ScheduleTest("select-json-by-non-indexed-value").
		ScheduleTest("select-ts-sql").
		ScheduleTest("select-heavy-min-max-tenant").
		ScheduleTest("select-heavy-min-max-tenant-and-state").
		SetParameters(func(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
			b.CommonOpts.Duration = 10
			b.CommonOpts.Workers = workers
			b.CommonOpts.Loops = 0
		}).
		ScheduleTest("select-heavy-last-tenant").
		ScheduleTest("select-heavy-rand-tenant-like").
		ScheduleTest("select-heavy-last-tenant-cti").
		ScheduleTest("select-json-by-indexed-value").
		ScheduleTest("select-json-by-non-indexed-value").
		ScheduleTest("select-ts-sql").
		ScheduleTest("select-heavy-min-max-tenant").
		ScheduleTest("select-heavy-min-max-tenant-and-state")

	if err := engine.RegisterPerfSuite("all", perfSuiteAllBasic); err != nil {
		panic(err)
	}
}
