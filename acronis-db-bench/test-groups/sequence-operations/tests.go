package sequence_operations

import (
	"context"

	"github.com/acronis/perfkit/benchmark"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Sequence operations tests group")

	// Sequence operation tests
	tg.Add(&TestSelectNextVal)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

const SequenceName = "acronis_db_bench_sequence" // SequenceName is the name of the sequence used for generating IDs

// TestSelectNextVal tests increment a DB sequence in a loop (or use SELECT FOR UPDATE, UPDATE)
var TestSelectNextVal = engine.TestDesc{
	Name:        "select-nextval",
	Metric:      "ops/sec",
	Description: "increment a DB sequence in a loop (or use SELECT FOR UPDATE, UPDATE)",
	Category:    engine.TestOther,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.RELATIONAL,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		c := engine.DbConnector(b)
		c.Database.CreateSequence(SequenceName)

		worker := func(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
			var explain = b.TestOpts.(*engine.TestOpts).DBOpts.Explain
			var session = c.Database.Session(c.Database.Context(context.Background(), explain))
			if _, err := session.GetNextVal(SequenceName); err != nil {
				b.Exit(err)
			}

			return 1
		}

		engine.TestGeneric(b, testDesc, worker, 0)
	},
}
