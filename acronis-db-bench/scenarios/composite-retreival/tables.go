package composite_retreival

import (
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// TestTableEmailSecurity is table to store email security objects
var TestTableEmailSecurity = engine.TestTable{
	TableName: "acronis_db_bench_email_security",
	Databases: []db.DialectName{db.ELASTICSEARCH, db.OPENSEARCH},
	Columns: [][]interface{}{
		{"id", "dataset.id"},
		{"date", "dataset.Date"},
		{"sender", "dataset.From"},
		{"recipient", "dataset.To"},
		{"subject", "dataset.Subject"},
		{"body", "dataset.Body"},
		{"embedding", "dataset.Embedding.list.element"},
	},
	TableDefinition: func(dialect db.DialectName) *db.TableDefinition {
		var tableRows []db.TableRow

		tableRows = append(tableRows,
			db.TableRow{Name: "id", Type: db.DataTypeBigInt, Indexed: true},
			db.TableRow{Name: "date", Type: db.DataTypeDateTime, Indexed: true},
			db.TableRow{Name: "sender", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRow{Name: "recipient", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRow{Name: "subject", Type: db.DataTypeVarChar, Indexed: true},
			db.TableRow{Name: "body", Type: db.DataTypeText, Indexed: true},
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
