package main

import (
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/acronis/perfkit/benchmark"
)

/*
 * Worker initialization
 */

func initWorker(b *benchmark.Benchmark, workerID int, testDesc *TestDesc, rowsRequired uint64) {
	if b.WorkerData[workerID] == nil {
		var workerData DBWorkerData
		workerData.conn = benchmark.NewDBConnector(&b.TestOpts.(*TestOpts).DBOpts, workerID, b.Logger, 10)
		b.WorkerData[workerID] = &workerData
		if testDesc.isDBRTest {
			workerData.conn.DBRConnect()
		} else {
			workerData.conn.Connect()
		}
	}

	if workerID == 0 {
		conn := b.WorkerData[0].(*DBWorkerData).conn
		testData := b.Vault.(*DBTestData)
		testData.TestDesc = testDesc

		tableName := testDesc.table.TableName

		t := TestTables[tableName]

		if tableName == "" {
			testDesc.table.RowsCount = 0
		} else {
			b.Log(benchmark.LogTrace, workerID, fmt.Sprintf("initializing table '%s'", tableName))
			if testDesc.isReadonly {
				b.Log(benchmark.LogTrace, workerID, fmt.Sprintf("readonly test, skipping table '%s' initialization", tableName))
				if !conn.TableExists(tableName) {
					b.Exit("The '%s' table doesn't exist, please create tables using -I option, or use individual insert test using the -t `insert-***`", tableName)
				}
			} else {
				b.Log(benchmark.LogTrace, workerID, fmt.Sprintf("creating table '%s'", tableName))
				t.Create(conn, b)
			}

			testDesc.table.RowsCount = conn.GetRowsCount(tableName, "")
			b.Log(benchmark.LogInfo, workerID, fmt.Sprintf("table '%s' has %d rows", tableName, testDesc.table.RowsCount))

			if rowsRequired > 0 {
				if testDesc.table.RowsCount < rowsRequired {
					b.Exit(fmt.Sprintf("table '%s' has %d rows, but this test requires at least %d rows, please insert it first and then re-run the test",
						testDesc.table.TableName, testDesc.table.RowsCount, rowsRequired))
				}
			}
		}

		b.TenantsCache.Init(conn)
	}
	b.Log(benchmark.LogTrace, workerID, "worker is initialized")
	b.WorkerData[workerID].(*DBWorkerData).conn.SetLogLevel(benchmark.LogInfo)
}

func initCommon(b *benchmark.Benchmark, testDesc *TestDesc, rowsRequired uint64) {
	b.InitPerWorker = func(workerId int) {
		initWorker(b, workerId, testDesc, rowsRequired)
	}

	b.Metric = func() (metric string) {
		return testDesc.metric
	}

	b.FinishPerWorker = func(worker_id int) {
		conn := b.WorkerData[worker_id].(*DBWorkerData).conn
		conn.SetLogLevel(benchmark.LogTrace)
		conn.Release()
	}
}

/*
 * SELECT workers
 */

func testGeneric(b *benchmark.Benchmark, testDesc *TestDesc, workerFunc testWorkerFunc, rowsRequired uint64) {
	initCommon(b, testDesc, rowsRequired)

	b.Worker = func(workerId int) (loops int) {
		c := b.WorkerData[workerId].(*DBWorkerData).conn
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
	what string,
	whereFunc func(b *benchmark.Benchmark, workerId int) string,
	orderByFunc func(b *benchmark.Benchmark) string,
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
		c := b.WorkerData[workerId].(*DBWorkerData).conn

		from := testDesc.table.TableName
		if fromFunc != nil {
			from = fromFunc(b, workerId)
		}
		where := ""
		if whereFunc != nil {
			where = whereFunc(b, workerId)
		}
		orderBy := ""
		if orderByFunc != nil {
			orderBy = orderByFunc(b)
		}

		if testDesc.isDBRTest {
			var rows []row
			if explain {
				b.Exit("sorry, the 'explain' mode is not supported for DBR SELECT yet")
			}
			c.DBRSelect(from, what, where, orderBy, batch, &rows)

			return batch
		}
		c.Select(from, what, where, orderBy, batch, explain)

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}

/*
 * INSERT worker
 */

func getDBDriver(b *benchmark.Benchmark) string {
	return b.TestOpts.(*TestOpts).DBOpts.Driver
}

func testInsertGeneric(b *benchmark.Benchmark, testDesc *TestDesc) {
	colConfs := testDesc.table.GetColumnsForInsert(benchmark.WithAutoInc(getDBDriver(b)))

	if len(*colConfs) == 0 {
		b.Exit(fmt.Sprintf("internal error: no columns eligible for INSERT found in '%s' configuration", testDesc.table.TableName))
	}

	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.table

	testOpts, ok := b.TestOpts.(*TestOpts)
	if !ok {
		b.Exit("db type conversion error")
	}

	if b.TestOpts.(*TestOpts).DBOpts.Driver == benchmark.CLICKHOUSE {
		sql := fmt.Sprintf("INSERT INTO %s", table.TableName) //nolint:perfsprint
		b.Worker = func(workerId int) (loops int) {
			workerData := b.WorkerData[workerId].(*DBWorkerData)
			rows := table.RowsCount

			c := workerData.conn
			tx := c.Begin()
			txBatch, err := tx.Prepare(sql)
			if err != nil {
				c.Exit("Prepare failed: %v", err)
			}

			for i := 0; i < batch; i++ {
				// clickhouse doesn't support autoincremented ID, so need to maintain it here
				_, values := b.GenFakeData(workerId, colConfs, false)
				atomic.AddUint64(&rows, 1)
				args := append([]interface{}{rows}, values...)

				for n, v := range args {
					if t, ok := v.(benchmark.TenantUUID); ok {
						args[n] = string(t)
					}
				}

				_, err := txBatch.Exec(args...)
				if err != nil {
					c.Exit("can't exec: %s", err)
				}

				if c.Logger.LogLevel >= benchmark.LogDebug {
					c.Log(benchmark.LogDebug, fmt.Sprintf("%s %v", sql, args))
				}
			}
			defer func() {
				if err := txBatch.Close(); err != nil {
					c.Exit("Close() error: %s", err)
				}
			}()

			c.Commit()

			return batch
		}
	} else if testDesc.isDBRTest {
		b.Worker = func(workerId int) (loops int) {
			var t time.Time
			if b.Logger.LogLevel >= benchmark.LogDebug {
				t = time.Now()
			}

			c := b.WorkerData[workerId].(*DBWorkerData).conn
			tx, err := c.DbrSess().Begin()
			b.Log(benchmark.LogDebug, workerId, "BEGIN")
			if err != nil {
				b.Exit(err.Error())
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				columns, values := b.GenFakeData(workerId, colConfs, false)
				_, err := tx.InsertInto(table.TableName).Columns(columns...).Values(values...).Exec()
				if err != nil {
					b.Exit("aborting")
				}
				c.DBRLogQuery(nil)
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
		insertSQL := "INSERT INTO %s (%s) VALUES(%s)"

		b.Worker = func(workerId int) (loops int) {
			workerData := b.WorkerData[workerId].(*DBWorkerData)
			parametersPlaceholder := benchmark.GenDBParameterPlaceholders(0, len(*colConfs))

			var sql string

			c := workerData.conn
			c.Begin()

			for i := 0; i < batch; i++ {
				columns, values := b.GenFakeData(workerId, colConfs, benchmark.WithAutoInc(getDBDriver(b)))

				if i == 0 {
					sqlTemplate := fmt.Sprintf(insertSQL, table.TableName, strings.Join(columns, ","), parametersPlaceholder)
					sql = formatSQL(sqlTemplate, testOpts.DBOpts.Driver)
				}

				c.ExecOrExit(sql, values...)

				if b.TestOpts.(*TestOpts).BenchOpts.Events {
					rw := b.Randomizer.GetWorker(workerId)
					b.Vault.(*DBTestData).EventBus.InsertEvent(rw, c, rw.UUID())
				}
			}
			c.Commit()

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
		colConfs = testDesc.table.GetColumnsForUpdate(benchmark.WithAutoInc(getDBDriver(b)))
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

			c := b.WorkerData[workerId].(*DBWorkerData).conn
			tx, err := c.DbrSess().Begin()
			b.Log(benchmark.LogDebug, workerId, "BEGIN")
			if err != nil {
				b.Exit(err.Error())
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				columns := b.GenFakeDataAsMap(workerId, colConfs, benchmark.WithAutoInc(getDBDriver(b)))
				id := int64(b.Randomizer.GetWorker(workerId).Uintn64(table.RowsCount - updateRows))

				var err error

				if updateRows == 1 {
					_, err = tx.Update(table.TableName).SetMap(*columns).Where(fmt.Sprintf("id > %d", id)).Exec()
				} else {
					_, err = tx.Update(table.TableName).SetMap(*columns).Where(fmt.Sprintf("id > %d AND id < %d", id, id+int64(updateRows))).Exec()
				}
				if err != nil {
					b.Exit("aborting")
				}
				c.DBRLogQuery(nil)
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

		driver := testOpts.DBOpts.Driver

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
		updateSQL := formatSQL(updateSQLTemplate, driver)

		b.Worker = func(workerId int) (loops int) {
			c := b.WorkerData[workerId].(*DBWorkerData).conn

			c.Begin()

			for i := 0; i < batch; i++ {
				id := int64(b.Randomizer.GetWorker(workerId).Uintn64(table.RowsCount-updateRows) + updateRows)
				_, values := b.GenFakeData(workerId, colConfs, false)

				values = append(values, id)
				if updateRows > 1 {
					values = append(values, id-int64(updateRows))
				}

				c.QueryAndReturnString(updateSQL, values...)

				if b.TestOpts.(*TestOpts).BenchOpts.Events {
					rw := b.Randomizer.GetWorker(workerId)
					b.Vault.(*DBTestData).EventBus.InsertEvent(rw, c, rw.UUID())
				}
			}
			c.Commit()

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

	if testDesc.isDBRTest {
		b.Worker = func(workerId int) (loops int) {
			var t time.Time
			if b.Logger.LogLevel >= benchmark.LogDebug {
				t = time.Now()
			}

			c := b.WorkerData[workerId].(*DBWorkerData).conn
			tx, err := c.DbrSess().Begin()
			b.Log(benchmark.LogDebug, workerId, "BEGIN")
			if err != nil {
				b.Exit(err.Error())
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				id := int64(b.Randomizer.GetWorker(workerId).Uintn64(table.RowsCount - deleteRows))

				var err error

				if deleteRows == 1 {
					_, err = tx.DeleteFrom(table.TableName).Where(fmt.Sprintf("id > %d", id)).Exec()
				} else {
					_, err = tx.DeleteFrom(table.TableName).Where(fmt.Sprintf("id > %d AND id < %d", id, id+int64(deleteRows))).Exec()
				}
				if err != nil {
					b.Exit("aborting")
				}
				c.DBRLogQuery(nil)
			}

			err = tx.Commit()
			if err != nil {
				b.Exit("Commit() error: %s", err)
			}

			if b.Logger.LogLevel >= benchmark.LogDebug {
				c.Log(benchmark.LogDebug, fmt.Sprintf("COMMIT # dur: %.6f", time.Since(t).Seconds()))
			}

			return batch * int(deleteRows)
		}
	} else {
		testOpts, ok := b.TestOpts.(*TestOpts)
		if !ok {
			b.Exit("db type conversion error")
		}

		var deleteSQLTemplate string
		if deleteRows == 1 {
			deleteSQLTemplate = fmt.Sprintf("DELETE FROM %s WHERE id = $1", table.TableName)
		} else {
			deleteSQLTemplate = fmt.Sprintf("DELETE FROM %s WHERE id <= $1 AND id > $2", table.TableName)
		}
		deleteSQL := formatSQL(deleteSQLTemplate, testOpts.DBOpts.Driver)

		b.Worker = func(workerId int) (loops int) {
			c := b.WorkerData[workerId].(*DBWorkerData).conn

			c.Begin()

			for i := 0; i < batch; i++ {
				id := int64(b.Randomizer.GetWorker(workerId).Uintn64(table.RowsCount-deleteRows) + deleteRows)
				var values []interface{}

				values = append(values, id)
				if deleteRows > 1 {
					values = append(values, id-int64(deleteRows))
				}

				c.QueryAndReturnString(deleteSQL, values...)

				if b.TestOpts.(*TestOpts).BenchOpts.Events {
					rw := b.Randomizer.GetWorker(workerId)
					b.Vault.(*DBTestData).EventBus.InsertEvent(rw, c, rw.UUID())
				}
			}
			c.Commit()

			return batch * int(deleteRows)
		}
	}

	b.Run()

	b.Vault.(*DBTestData).scores[testDesc.category] = append(b.Vault.(*DBTestData).scores[testDesc.category], b.Score)
}
