package vector_search

import (
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableVector768 is table to store 768-dimensions vector objects
var TestTableVector768 = engine.TestTable{
	TableName: "acronis_db_bench_vector_768",
	Databases: []db.DialectName{db.POSTGRES, db.ELASTICSEARCH, db.OPENSEARCH},
	Columns: [][]interface{}{
		{"id", "dataset.id"},
		{"embedding", "dataset.emb.list.item"},
	},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		var tableRows []db.TableRow

		tableRows = append(tableRows,
			db.TableRow{Name: "id", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRow{Name: "embedding", Type: db.DataTypeVector768Float32, Indexed: true},
		)

		var tableDef = &db.TableDefinition{
			TableRows: tableRows,
		}

		if dialect == db.ELASTICSEARCH {
			tableDef.Resilience.NumberOfReplicas = 2
		}

		return tableDef
	},
}
