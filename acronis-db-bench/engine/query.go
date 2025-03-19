package engine

import (
	"context"
	"fmt"
	"strings"

	"github.com/acronis/perfkit/benchmark"
)

// TestRawQuery tests do custom DB query execution
var TestRawQuery = TestDesc{
	Name:        "custom",
	Metric:      "queries/sec",
	Description: "custom DB query execution",
	Category:    TestOther,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   ALL,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *TestDesc) {
		query := b.TestOpts.(*TestOpts).BenchOpts.Query

		var worker TestWorkerFunc
		var explain = b.TestOpts.(*TestOpts).DBOpts.Explain

		if strings.Contains(query, "{") {
			worker = func(b *benchmark.Benchmark, c *DBConnector, testDesc *TestDesc, batch int) (loops int) { //nolint:revive
				q := query
				if strings.Contains(q, "{CTI}") {
					rw := b.Randomizer.GetWorker(c.WorkerID)
					ctiUUID, err := b.Vault.(*DBTestData).TenantsCache.GetRandomCTIUUID(rw, 0)
					if err != nil {
						b.Exit(err)
					}
					q = strings.Replace(q, "{CTI}", "'"+string(ctiUUID)+"'", -1)
				}
				if strings.Contains(query, "{TENANT}") {
					rw := b.Randomizer.GetWorker(c.WorkerID)
					tenantUUID, err := b.Vault.(*DBTestData).TenantsCache.GetRandomTenantUUID(rw, 0, "")
					if err != nil {
						b.Exit(err)
					}
					q = strings.Replace(q, "{TENANT}", "'"+string(tenantUUID)+"'", -1)
				}
				fmt.Printf("query %s\n", q)

				var session = c.Database.Session(c.Database.Context(context.Background(), explain))
				if _, err := session.Query(q); err != nil {
					b.Exit(err)
				}

				return 1
			}
		} else {
			worker = func(b *benchmark.Benchmark, c *DBConnector, testDesc *TestDesc, batch int) (loops int) {
				var session = c.Database.Session(c.Database.Context(context.Background(), explain))
				if _, err := session.Query(query); err != nil {
					b.Exit(err)
				}

				return 1
			}
		}
		TestGeneric(b, testDesc, worker, 0)
	},
}
