package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gocraft/dbr/v2"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	tenants "github.com/acronis/perfkit/acronis-db-bench/tenants-cache"
)

/*
 * Worker initialization
 */

func initWorker(b *benchmark.Benchmark, workerID int, testDesc *TestDesc, rowsRequired uint64) {
	if b.WorkerData[workerID] == nil {
		var workerData DBWorkerData
		var err error

		if workerData.workingConn, err = NewDBConnector(&b.TestOpts.(*TestOpts).DBOpts, workerID, b.Logger, 1); err != nil {
			return
		}

		if b.TestOpts.(*TestOpts).BenchOpts.TenantConnString != "" {
			var tenantCacheDBOpts = b.TestOpts.(*TestOpts).DBOpts
			tenantCacheDBOpts.ConnString = b.TestOpts.(*TestOpts).BenchOpts.TenantConnString

			if workerData.tenantsCache, err = NewDBConnector(&tenantCacheDBOpts, workerID, b.Logger, 1); err != nil {
				b.Exit("db: cannot create tenants cache connection: %v", err)
			}
		}

		b.WorkerData[workerID] = &workerData
	}

	if workerID == 0 {
		conn := b.WorkerData[0].(*DBWorkerData).workingConn
		testData := b.Vault.(*DBTestData)
		testData.TestDesc = testDesc

		tableName := testDesc.table.TableName

		t := TestTables[tableName]

		if tableName == "" {
			testDesc.table.RowsCount = 0
		} else {
			b.Log(benchmark.LogTrace, workerID, fmt.Sprintf("initializing table '%s'", tableName))
			if testDesc.isReadonly {
				t.Create(conn, b)
				b.Log(benchmark.LogTrace, workerID, fmt.Sprintf("readonly test, skipping table '%s' initialization", tableName))
				if exists, err := conn.database.TableExists(tableName); err != nil {
					b.Exit(fmt.Sprintf("db: cannot check if table '%s' exists: %v", tableName, err))
				} else if !exists {
					b.Exit("The '%s' table doesn't exist, please create tables using -I option, or use individual insert test using the -t `insert-***`", tableName)
				}
			} else {
				b.Log(benchmark.LogTrace, workerID, fmt.Sprintf("creating table '%s'", tableName))
				t.Create(conn, b)
			}

			var session = conn.database.Session(conn.database.Context(context.Background()))
			var rowNum int64
			if rows, err := session.Select(tableName, &db.SelectCtrl{Fields: []string{"COUNT(0)"}}); err != nil {
				b.Exit(fmt.Sprintf("db: cannot get rows count in table '%s': %v", tableName, err))
			} else {
				for rows.Next() {
					if scanErr := rows.Scan(&rowNum); scanErr != nil {
						b.Exit(fmt.Sprintf("db: cannot get rows count in table '%s': %v", tableName, scanErr))
					}
				}
				rows.Close()
			}

			testDesc.table.RowsCount = uint64(rowNum)
			b.Log(benchmark.LogInfo, workerID, fmt.Sprintf("table '%s' has %d rows", tableName, testDesc.table.RowsCount))

			if rowsRequired > 0 {
				if testDesc.table.RowsCount < rowsRequired {
					b.Exit(fmt.Sprintf("table '%s' has %d rows, but this test requires at least %d rows, please insert it first and then re-run the test",
						testDesc.table.TableName, testDesc.table.RowsCount, rowsRequired))
				}
			}
		}

		var tenantCacheDatabase db.Database
		if b.WorkerData[0].(*DBWorkerData).tenantsCache != nil {
			tenantCacheDatabase = b.WorkerData[0].(*DBWorkerData).tenantsCache.database
		} else {
			tenantCacheDatabase = conn.database
		}

		if err := b.Vault.(*DBTestData).TenantsCache.Init(tenantCacheDatabase); err != nil {
			b.Exit("db: cannot initialize tenants cache: %v", err)
		}
	}
	b.Log(benchmark.LogTrace, workerID, "worker is initialized")
}

func initCommon(b *benchmark.Benchmark, testDesc *TestDesc, rowsRequired uint64) {
	b.InitPerWorker = func(workerId int) {
		initWorker(b, workerId, testDesc, rowsRequired)
	}

	b.Metric = func() (metric string) {
		return testDesc.metric
	}

	b.FinishPerWorker = func(workerId int) {
		conn := b.WorkerData[workerId].(*DBWorkerData).workingConn
		conn.Release()
	}
}

/*
 * SELECT workers
 */

func testGeneric(b *benchmark.Benchmark, testDesc *TestDesc, workerFunc testWorkerFunc, rowsRequired uint64) {
	initCommon(b, testDesc, rowsRequired)

	b.Worker = func(workerId int) (loops int) {
		c := b.WorkerData[workerId].(*DBWorkerData).workingConn
		batch := b.Vault.(*DBTestData).EffectiveBatch

		return workerFunc(b, c, testDesc, batch)
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

func testSelect(
	b *benchmark.Benchmark,
	testDesc *TestDesc,
	fromFunc func(b *benchmark.Benchmark, workerId int) string,
	what []string,
	whereFunc func(b *benchmark.Benchmark, workerId int) map[string][]string,
	orderByFunc func(b *benchmark.Benchmark, workerId int) []string,
	rowsRequired uint64,
) {
	initCommon(b, testDesc, rowsRequired)
	testOpts, ok := b.TestOpts.(*TestOpts)
	if !ok {
		b.Exit("TestOpts type conversion error")
	}

	explain := testOpts.BenchOpts.Explain

	batch := b.Vault.(*DBTestData).EffectiveBatch

	type row struct {
		ID int64 `db:"id"`
	}

	b.Worker = func(workerId int) (loops int) {
		c := b.WorkerData[workerId].(*DBWorkerData).workingConn

		from := testDesc.table.TableName
		if fromFunc != nil {
			from = fromFunc(b, workerId)
		}

		var whereCond map[string][]string
		if whereFunc != nil {
			whereCond = whereFunc(b, workerId)
		}

		var orderBy []string
		if orderByFunc != nil {
			orderBy = orderByFunc(b, workerId)
		}

		if testDesc.isDBRTest {
			if rawSession, casted := c.database.RawSession().(*dbr.Session); casted {
				var rows []row
				if explain {
					b.Exit("sorry, the 'explain' mode is not supported for DBR SELECT yet")
				}

				var q = rawSession.Select("*").From(from).Where("id = ?", 1).Limit(uint64(batch))

				/*
					if orderBy != "" {
						q = q.OrderBy(orderBy)
					}

					if where != "" {
						q = q.Where(where)
					}
				*/

				_, err := q.Load(rows)
				if err != nil {
					c.Exit("DBRSelect load error: %v: from: %s, what: %s, orderBy: %s, limit: %d", err, from, what, orderBy, batch)
				}

				return batch
			}
		}

		var session = c.database.Session(c.database.Context(context.Background()))
		var rows, err = session.Select(from, &db.SelectCtrl{
			Fields: what,
			Where:  whereCond,
			Order:  orderBy,
			Page: db.Page{
				Limit: int64(batch),
			},
			OptimizeConditions: false,
		})
		if err != nil {
			b.Exit("db: cannot select rows: %v", err)
		}

		for rows.Next() {
			if err != nil {
				b.Exit(err)
			}
		}

		rows.Close()

		return 1
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

/*
 * INSERT worker
 */

func getDBDriver(b *benchmark.Benchmark) db.DialectName {
	var dialectName, err = db.GetDialectName(b.TestOpts.(*TestOpts).DBOpts.ConnString)
	if err != nil {
		b.Exit(err)
	}

	return dialectName
}

func testInsertGeneric(b *benchmark.Benchmark, testDesc *TestDesc) {
	colConfs := testDesc.table.GetColumnsForInsert(db.WithAutoInc(getDBDriver(b)))

	if len(*colConfs) == 0 {
		b.Exit(fmt.Sprintf("internal error: no columns eligible for INSERT found in '%s' configuration", testDesc.table.TableName))
	}

	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	var dialectName, dialErr = db.GetDialectName(b.TestOpts.(*TestOpts).DBOpts.ConnString)
	if dialErr != nil {
		b.Exit(dialErr)
	}

	if dialectName == db.CLICKHOUSE {
		sql := fmt.Sprintf("INSERT INTO %s", table.TableName) //nolint:perfsprint

		b.Worker = func(workerId int) (loops int) {
			workerData := b.WorkerData[workerId].(*DBWorkerData)
			rows := table.RowsCount

			var c = workerData.workingConn
			var sess = c.database.Session(c.database.Context(context.Background()))

			if txErr := sess.Transact(func(tx db.DatabaseAccessor) error {
				var txBatch, prepareErr = tx.Prepare(sql)
				if prepareErr != nil {
					return prepareErr
				}

				for i := 0; i < batch; i++ {
					// clickhouse doesn't support autoincremented ID, so need to maintain it here
					_, values := b.GenFakeData(workerId, colConfs, false)
					atomic.AddUint64(&rows, 1)
					args := append([]interface{}{rows}, values...)

					for n, v := range args {
						if t, ok := v.(tenants.TenantUUID); ok {
							args[n] = string(t)
						}
					}

					if _, err := txBatch.Exec(args...); err != nil {
						return err
					}

					if c.Logger.LogLevel >= benchmark.LogDebug {
						c.Log(benchmark.LogDebug, fmt.Sprintf("%s %v", sql, args))
					}
				}

				defer txBatch.Close()

				return nil
			}); txErr != nil {
				b.Exit(txErr.Error())
			}

			return batch
		}
	} else if testDesc.isDBRTest {
		b.Worker = func(workerId int) (loops int) {
			var t time.Time
			if b.Logger.LogLevel >= benchmark.LogDebug {
				t = time.Now()
			}

			c := b.WorkerData[workerId].(*DBWorkerData).workingConn

			var rawDbrSess = c.database.RawSession()
			var dbrSess = rawDbrSess.(*dbr.Session)

			tx, err := dbrSess.Begin()
			b.Log(benchmark.LogDebug, workerId, "BEGIN")
			if err != nil {
				b.Exit(err.Error())
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				columns, values := b.GenFakeData(workerId, colConfs, false)
				_, err = tx.InsertInto(table.TableName).Columns(columns...).Values(values...).Exec()
				if err != nil {
					b.Exit("aborting")
				}
			}

			err = tx.Commit()
			if err != nil {
				b.Exit("Commit() error: %s", err)
			}

			if b.Logger.LogLevel >= benchmark.LogDebug {
				c.Log(benchmark.LogDebug, fmt.Sprintf("COMMIT # dur: %.6f", time.Since(t).Seconds()))
			}

			return batch
		}
	} else {
		b.Worker = func(workerId int) (loops int) {
			workerData := b.WorkerData[workerId].(*DBWorkerData)

			var c = workerData.workingConn
			var sess = c.database.Session(c.database.Context(context.Background()))

			if txErr := sess.Transact(func(tx db.DatabaseAccessor) error {
				for i := 0; i < batch; i++ {
					columns, values := b.GenFakeData(workerId, colConfs, db.WithAutoInc(getDBDriver(b)))

					if err := tx.BulkInsert(table.TableName, [][]interface{}{values}, columns); err != nil {
						return err
					}

					if b.TestOpts.(*TestOpts).BenchOpts.Events {
						rw := b.Randomizer.GetWorker(workerId)
						if err := b.Vault.(*DBTestData).EventBus.InsertEvent(rw, tx, rw.UUID().String()); err != nil {
							return err
						}
					}
				}

				return nil
			}); txErr != nil {
				b.Exit(txErr.Error())
			}

			return batch
		}
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

/*
 * UPDATE worker
 */

func testUpdateGeneric(b *benchmark.Benchmark, testDesc *TestDesc, updateRows uint64, colConfs *[]benchmark.DBFakeColumnConf) {
	if colConfs == nil {
		colConfs = testDesc.table.GetColumnsForUpdate(db.WithAutoInc(getDBDriver(b)))
	}

	if len(*colConfs) == 0 {
		b.Exit(fmt.Sprintf("internal error: no columns eligible for UPDATE found in '%s' configuration", testDesc.table.TableName))
	}

	initCommon(b, testDesc, updateRows)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	if testDesc.isDBRTest {
		b.Worker = func(workerId int) (loops int) {
			var t time.Time
			if b.Logger.LogLevel >= benchmark.LogDebug {
				t = time.Now()
			}

			c := b.WorkerData[workerId].(*DBWorkerData).workingConn

			var rawDbrSess = c.database.RawSession()
			var dbrSess = rawDbrSess.(*dbr.Session)

			tx, err := dbrSess.Begin()
			b.Log(benchmark.LogDebug, workerId, "BEGIN")
			if err != nil {
				b.Exit(err.Error())
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				columns := b.GenFakeDataAsMap(workerId, colConfs, db.WithAutoInc(getDBDriver(b)))
				id := int64(b.Randomizer.GetWorker(workerId).Uintn64(table.RowsCount - updateRows))

				if updateRows == 1 {
					_, err = tx.Update(table.TableName).SetMap(*columns).Where(fmt.Sprintf("id > %d", id)).Exec()
				} else {
					_, err = tx.Update(table.TableName).SetMap(*columns).Where(fmt.Sprintf("id > %d AND id < %d", id, id+int64(updateRows))).Exec()
				}
				if err != nil {
					b.Exit("aborting")
				}
			}

			err = tx.Commit()
			if err != nil {
				b.Exit("Commit() error: %s", err)
			}

			if b.Logger.LogLevel >= benchmark.LogDebug {
				c.Log(benchmark.LogDebug, fmt.Sprintf("COMMIT # dur: %.6f", time.Since(t).Seconds()))
			}

			return batch * int(updateRows)
		}
	} else {
		testOpts, ok := b.TestOpts.(*TestOpts)
		if !ok {
			b.Exit("db type conversion error")
		}

		var dialectName, err = db.GetDialectName(testOpts.DBOpts.ConnString)
		if err != nil {
			b.Exit(err)
		}

		values := make([]string, len(*colConfs))
		for i := 0; i < len(*colConfs); i++ {
			values[i] = fmt.Sprintf("%s = $%d", (*colConfs)[i].ColumnName, i+1)
		}
		setPart := strings.Join(values, ", ")

		var updateSQLTemplate string
		if updateRows == 1 {
			updateSQLTemplate = fmt.Sprintf("UPDATE %s SET %s WHERE id = $%d", table.TableName, setPart, len(*colConfs)+1)
		} else {
			updateSQLTemplate = fmt.Sprintf("UPDATE %s SET %s WHERE id <= $%d AND id > $%d", table.TableName, setPart, len(*colConfs)+1, len(*colConfs)+2)
		}
		updateSQL := formatSQL(updateSQLTemplate, dialectName)

		b.Worker = func(workerId int) (loops int) {
			var c = b.WorkerData[workerId].(*DBWorkerData).workingConn
			var session = c.database.Session(c.database.Context(context.Background()))
			if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
				for i := 0; i < batch; i++ {
					id := int64(b.Randomizer.GetWorker(workerId).Uintn64(table.RowsCount-updateRows) + updateRows)
					_, fakeDataValues := b.GenFakeData(workerId, colConfs, false)

					fakeDataValues = append(fakeDataValues, id)
					if updateRows > 1 {
						fakeDataValues = append(fakeDataValues, id-int64(updateRows))
					}

					if _, err = tx.Exec(updateSQL, fakeDataValues...); err != nil {
						return err
					}

					if b.TestOpts.(*TestOpts).BenchOpts.Events {
						rw := b.Randomizer.GetWorker(workerId)
						if err = b.Vault.(*DBTestData).EventBus.InsertEvent(rw, tx, rw.UUID().String()); err != nil {
							return err
						}
					}
				}

				return nil
			}); txErr != nil {
				b.Exit(txErr.Error())
			}

			return batch * int(updateRows)
		}
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

/*
 * DELETE worker
 */
// testDeleteGeneric is a generic DELETE worker
func testDeleteGeneric(b *benchmark.Benchmark, testDesc *TestDesc, deleteRows uint64) { //nolint:unused
	initCommon(b, testDesc, deleteRows)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	var dialectName, err = db.GetDialectName(b.TestOpts.(*TestOpts).DBOpts.ConnString)
	if err != nil {
		b.Exit(err)
	}

	if testDesc.isDBRTest {
		b.Worker = func(workerId int) (loops int) {
			var t time.Time
			if b.Logger.LogLevel >= benchmark.LogDebug {
				t = time.Now()
			}

			c := b.WorkerData[workerId].(*DBWorkerData).workingConn

			var rawDbrSess = c.database.RawSession()
			var dbrSess = rawDbrSess.(*dbr.Session)

			tx, err := dbrSess.Begin()
			b.Log(benchmark.LogDebug, workerId, "BEGIN")
			if err != nil {
				b.Exit(err.Error())
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				id := int64(b.Randomizer.GetWorker(workerId).Uintn64(table.RowsCount - deleteRows))

				if deleteRows == 1 {
					_, err = tx.DeleteFrom(table.TableName).Where(fmt.Sprintf("id > %d", id)).Exec()
				} else {
					_, err = tx.DeleteFrom(table.TableName).Where(fmt.Sprintf("id > %d AND id < %d", id, id+int64(deleteRows))).Exec()
				}
				if err != nil {
					b.Exit("aborting")
				}
			}

			if err = tx.Commit(); err != nil {
				c.Exit(err.Error())
			}

			if b.Logger.LogLevel >= benchmark.LogDebug {
				c.Log(benchmark.LogDebug, fmt.Sprintf("COMMIT # dur: %.6f", time.Since(t).Seconds()))
			}

			return batch * int(deleteRows)
		}
	} else {
		var deleteSQLTemplate string
		if deleteRows == 1 {
			deleteSQLTemplate = fmt.Sprintf("DELETE FROM %s WHERE id = $1", table.TableName)
		} else {
			deleteSQLTemplate = fmt.Sprintf("DELETE FROM %s WHERE id <= $1 AND id > $2", table.TableName)
		}
		deleteSQL := formatSQL(deleteSQLTemplate, dialectName)

		b.Worker = func(workerId int) (loops int) {
			var c = b.WorkerData[workerId].(*DBWorkerData).workingConn
			var session = c.database.Session(c.database.Context(context.Background()))
			if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
				for i := 0; i < batch; i++ {
					id := int64(b.Randomizer.GetWorker(workerId).Uintn64(table.RowsCount-deleteRows) + deleteRows)
					var values []interface{}

					values = append(values, id)
					if deleteRows > 1 {
						values = append(values, id-int64(deleteRows))
					}

					if _, err := tx.Query(deleteSQL, values...); err != nil {
						return err
					}

					if b.TestOpts.(*TestOpts).BenchOpts.Events {
						rw := b.Randomizer.GetWorker(workerId)
						if err := b.Vault.(*DBTestData).EventBus.InsertEvent(rw, tx, rw.UUID().String()); err != nil {
							return err
						}
					}
				}

				return nil
			}); txErr != nil {
				b.Exit(txErr.Error())
			}

			return batch * int(deleteRows)
		}
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}
