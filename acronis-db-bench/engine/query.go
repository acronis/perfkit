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

		// Fix for connection leaks: always close the rows object returned from a query
		// to ensure the connection is properly released back to the pool
		if strings.Contains(query, "{") {
			worker = func(b *benchmark.Benchmark, c *DBConnector, testDesc *TestDesc, batch int) (loops int) { //nolint:revive
				q := query
				if strings.Contains(q, "{CTI}") {
					rz := b.Randomizer
					ctiUUID, err := b.Vault.(*DBTestData).TenantsCache.GetRandomCTIUUID(rz, 0)
					if err != nil {
						b.Exit(err)
					}
					q = strings.Replace(q, "{CTI}", "'"+string(ctiUUID)+"'", -1)
				}
				if strings.Contains(query, "{TENANT}") {
					rz := b.Randomizer
					tenantUUID, err := b.Vault.(*DBTestData).TenantsCache.GetRandomTenantUUID(rz, 0, "")
					if err != nil {
						b.Exit(err)
					}
					q = strings.Replace(q, "{TENANT}", "'"+string(tenantUUID)+"'", -1)
				}
				fmt.Printf("query %s\n", q)

				var session = c.Database.Session(c.Database.Context(context.Background(), explain))
				rows, err := session.Query(q)
				if err != nil {
					b.Exit(err)
				}
				defer rows.Close()

				return 1
			}
		} else {
			worker = func(b *benchmark.Benchmark, c *DBConnector, testDesc *TestDesc, batch int) (loops int) {
				var session = c.Database.Session(c.Database.Context(context.Background(), explain))
				rows, err := session.Query(query)
				if err != nil {
					b.Exit(err)
				}
				defer rows.Close()

				return 1
			}
		}
		TestGeneric(b, testDesc, worker, 0)
	},
}
