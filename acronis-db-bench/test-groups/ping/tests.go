package ping

import (
	"context"

	"github.com/acronis/perfkit/benchmark"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Ping tests group")

	// Ping tests
	tg.Add(&TestPing)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

// TestPing tests just ping DB
var TestPing = engine.TestDesc{
	Name:        "ping",
	Metric:      "ping/sec",
	Description: "just ping DB",
	Category:    engine.TestOther,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		worker := func(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
			if err := c.Database.Ping(context.Background()); err != nil {
				return 0
			}

			return 1
		}
		engine.TestGeneric(b, testDesc, worker, 0)
	},
}
