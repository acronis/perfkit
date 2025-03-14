package select_one

import (
	"context"
	"database/sql"
	"strings"

	es8 "github.com/elastic/go-elasticsearch/v8"
	"github.com/gocraft/dbr/v2"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

func init() {
	tests := []*engine.TestDesc{
		// Basic connectivity tests
		&TestSelectOne,
	}

	tables := map[string]engine.TestTable{}

	scenario := &engine.TestScenario{
		Name:   "select-one",
		Tests:  tests,
		Tables: tables,
	}

	if err := engine.RegisterTestScenario(scenario); err != nil {
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
			var ret int
			switch rawSession := c.Database.RawSession().(type) {
			case *dbr.Session:
				if err := rawSession.Select("1").LoadOne(&ret); err != nil {
					b.Exit("DBRSelect load error: %v", err)
				}
			case *sql.DB:
				if err := rawSession.QueryRow("SELECT 1").Scan(&ret); err != nil {
					if c.Database.DialectName() == db.CASSANDRA {
						// Cassandra driver returns error on SELECT 1
						return 1
					}
					b.Exit("can't do 'SELECT 1': %v", err)
				}
			case *es8.Client:
				var res, err = rawSession.Search(
					rawSession.Search.WithContext(context.Background()),
					rawSession.Search.WithBody(strings.NewReader(`{"size": 1}`)),
				)
				if err != nil {
					b.Exit("can't do 'SELECT 1': %v", err)
				}

				// nolint: errcheck // Need to have logger here for deferred errors
				defer res.Body.Close()

				if res.IsError() {
					if res.StatusCode != 404 {
						b.Exit("failed to perform search: %s", res.String())
					}
				}

				if res.StatusCode != 200 {
					b.Exit("failed to perform search: %s", res.String())
				}
			default:
				b.Exit("unknown driver: '%v', supported drivers are: postgres|sqlite|mysql|mssql", c.Database.DialectName())
			}

			return 1
		}
		engine.TestGeneric(b, testDesc, worker, 0)
	},
}
