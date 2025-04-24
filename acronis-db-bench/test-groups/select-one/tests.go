package select_one

import (
	"context"
	"fmt"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	var tg = engine.NewTestGroup("Select one tests group")

	// Basic connectivity tests
	tg.Add(&TestSelectOne)

	if err := engine.RegisterTestGroup(tg); err != nil {
		panic(err)
	}
}

// TestSelectOne tests do 'SELECT 1'
var TestSelectOne = engine.TestDesc{
	Name:        "select-1",
	Metric:      "select/sec",
	Description: "just do 'SELECT 1'",
	Category:    engine.TestSelect,
	IsReadonly:  true,
	IsDBRTest:   false,
	Databases:   engine.ALL,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		worker := func(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) { //nolint:revive
			var session = c.Database.Session(c.Database.Context(context.Background(), false))
			if rows, err := session.Select("", &db.SelectCtrl{Fields: []string{"1"}}); err != nil {
				b.Exit(fmt.Sprintf("db: cannot make SELECT 1: %v", err))
			} else {
				defer rows.Close()

				switch c.Database.DialectName() {
				case db.CASSANDRA, db.ELASTICSEARCH, db.OPENSEARCH:
					// These drivers returns empty rows on SELECT 1
					return 1
				}

				var ret int
				for rows.Next() {
					if scanErr := rows.Scan(&ret); scanErr != nil {
						b.Exit(fmt.Sprintf("db: cannot get 1 after SELECT 1: %v", err))
					}
				}
			}

			return 1
		}
		engine.TestGeneric(b, testDesc, worker, 0)
	},
}
