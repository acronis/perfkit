package large_objects_operations

import (
	"context"
	"fmt"
	"strings"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"
)

// createLargeObjectWorker inserts a row with large random object into the 'largeobject' table
func createLargeObjectWorker(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) {
	colConfs := testDesc.Table.GetColumnsForInsert(db.WithAutoInc(c.Database.DialectName()))
	parametersPlaceholder := db.GenDBParameterPlaceholders(0, len(*colConfs))
	workerID := c.WorkerID

	var session = c.Database.Session(c.Database.Context(context.Background(), false))

	if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
		var sql string
		for i := 0; i < batch; i++ {
			columns, values := b.GenFakeData(workerID, colConfs, false)

			blob := b.GenFakeValue(workerID, "blob", "", 0, b.TestOpts.(*engine.TestOpts).TestcaseOpts.MaxBlobSize, b.TestOpts.(*engine.TestOpts).TestcaseOpts.MinBlobSize, nil)

			var oid int
			if err := tx.QueryRow("SELECT lo_create(0)").Scan(&oid); err != nil {
				return err
			}

			var fd int
			if err := tx.QueryRow(fmt.Sprintf("SELECT lo_open(%d, 131072)", oid)).Scan(&fd); err != nil { // 131072 == 0x20000 - write mode
				return err
			}

			if _, err := tx.Exec("SELECT lowrite($1, $2)", fd, blob); err != nil {
				return err
			}

			if _, err := tx.Exec("SELECT lo_close($1)", fd); err != nil {
				return err
			}

			for col := range columns {
				if columns[col] == "oid" {
					values[col] = oid
				}
			}

			if i == 0 {
				insertSQL := "INSERT INTO %s (%s) VALUES(%s)"
				sqlTemplate := fmt.Sprintf(insertSQL, testDesc.Table.TableName, strings.Join(columns, ","), parametersPlaceholder)
				sql = engine.FormatSQL(sqlTemplate, c.Database.DialectName())
			}

			if _, err := tx.Exec(sql, values...); err != nil {
				return err
			}
		}

		return nil
	}); txErr != nil {
		c.Exit(txErr.Error())
	}

	return batch
}

// TestInsertLargeObj inserts a row with large random object into the 'largeobject' table
var TestInsertLargeObj = engine.TestDesc{
	Name:        "insert-largeobj",
	Metric:      "rows/sec",
	Description: "insert a row with large random object into the 'largeobject' table",
	Category:    engine.TestInsert,
	IsReadonly:  false,
	IsDBRTest:   false,
	Databases:   []db.DialectName{db.POSTGRES},
	Table:       TestTableLargeObj,
	LauncherFunc: func(b *benchmark.Benchmark, testDesc *engine.TestDesc) {
		engine.TestGeneric(b, testDesc, createLargeObjectWorker, 0)
	},
}
