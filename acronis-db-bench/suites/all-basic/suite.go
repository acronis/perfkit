package all_basic

import (
	"github.com/acronis/perfkit/benchmark"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func executeOneTest(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
	// Get current dialect
	var dialectName = getDBDriver(b)

	// Skip if current dialect is not supported by this test
	dialectSupported := false
	for _, supportedDialect := range testDesc.Databases {
		if dialectName == supportedDialect {
			dialectSupported = true
			break
		}
	}

	if !dialectSupported {
		// b.Log(benchmark.LogInfo, "Skipping test '%s' - not supported for dialect '%s'", testDesc.name, dialectName)
		return
	}

	testDesc.LauncherFunc(b, testDesc)

	// b.Log(benchmark.LogInfo, "Test '%s' completed", testDesc.name)
	select {
	case <-b.ShutdownCh:
		b.Logger.Debug("Gracefully stopping test execution...")
		b.Exit()
	default:
		if b.NeedToExit {
			b.Exit()
		}
	}
}

func executeAllTestsOnce(b *benchmark.Benchmark, testOpts *engine.TestOpts, workers int) {
	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectOne)

	/* Insert */

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 10000
	executeOneTest(b, &TestInsertTenant)

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 1000
	executeOneTest(b, &TestInsertCTI)

	//	b.CommonOpts.Duration = 0
	//	b.CommonOpts.Workers = workers
	//	b.CommonOpts.Loops = 30000
	//	executeOneTest(b, &TestInsertTenant)

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 5
	executeOneTest(b, &TestInsertLight)
	executeOneTest(b, &TestInsertMedium)
	executeOneTest(b, &TestInsertHeavy)
	executeOneTest(b, &TestInsertJSON)
	executeOneTest(b, &TestInsertTimeSeriesSQL)

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 95
	executeOneTest(b, &TestInsertLight)
	executeOneTest(b, &TestInsertMedium)
	executeOneTest(b, &TestInsertJSON)
	executeOneTest(b, &TestInsertTimeSeriesSQL)

	/* Update */

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 2
	executeOneTest(b, &TestUpdateMedium)
	executeOneTest(b, &TestUpdateHeavy)
	executeOneTest(b, &TestUpdateHeavyPartialSameVal)
	executeOneTest(b, &TestUpdateHeavySameVal)

	b.CommonOpts.Duration = 0
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = testOpts.BenchOpts.Chunk / 100 * 28
	executeOneTest(b, &TestUpdateMedium)
	executeOneTest(b, &TestUpdateHeavy)
	executeOneTest(b, &TestUpdateHeavyPartialSameVal)
	executeOneTest(b, &TestUpdateHeavySameVal)

	/* Select */

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectMediumRand)
	executeOneTest(b, &TestSelectHeavyRand)

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectMediumRand)
	executeOneTest(b, &TestSelectHeavyRand)

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectMediumLast)
	executeOneTest(b, &TestSelectHeavyLast)

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectMediumLast)
	executeOneTest(b, &TestSelectHeavyLast)

	/* Other select's */

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = 1
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectHeavyLastTenant)
	executeOneTest(b, &TestSelectHeavyRandTenantLike)
	executeOneTest(b, &TestSelectHeavyLastTenantCTI)
	executeOneTest(b, &TestSelectJSONByIndexedValue)
	executeOneTest(b, &TestSelectJSONByNonIndexedValue)
	executeOneTest(b, &TestSelectTimeSeriesSQL)
	executeOneTest(b, &TestSelectHeavyMinMaxTenant)
	executeOneTest(b, &TestSelectHeavyMinMaxTenantAndState)

	b.CommonOpts.Duration = 10
	b.CommonOpts.Workers = workers
	b.CommonOpts.Loops = 0
	executeOneTest(b, &TestSelectHeavyLastTenant)
	executeOneTest(b, &TestSelectHeavyRandTenantLike)
	executeOneTest(b, &TestSelectHeavyLastTenantCTI)
	executeOneTest(b, &TestSelectJSONByIndexedValue)
	executeOneTest(b, &TestSelectJSONByNonIndexedValue)
	executeOneTest(b, &TestSelectTimeSeriesSQL)
	executeOneTest(b, &TestSelectHeavyMinMaxTenant)
	executeOneTest(b, &TestSelectHeavyMinMaxTenantAndState)
}
