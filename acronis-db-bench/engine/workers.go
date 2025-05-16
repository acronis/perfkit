package engine

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync/atomic"
	"time"

	"github.com/gocraft/dbr/v2"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/logger"

	events "github.com/acronis/perfkit/acronis-db-bench/event-bus"
	tenants "github.com/acronis/perfkit/acronis-db-bench/tenants-cache"
)

/*
 * Worker initialization
 */

func initGeneric(b *benchmark.Benchmark, testDesc *TestDesc, rowsRequired uint64) {
	b.Vault.(*DBTestData).TenantsCache = tenants.NewTenantsCache(b)

	b.Vault.(*DBTestData).TenantsCache.SetTenantsWorkingSet(b.TestOpts.(*TestOpts).BenchOpts.TenantsWorkingSet)
	b.Vault.(*DBTestData).TenantsCache.SetCTIsWorkingSet(b.TestOpts.(*TestOpts).BenchOpts.CTIsWorkingSet)

	var tenantCacheDBOpts = b.TestOpts.(*TestOpts).DBOpts
	if b.TestOpts.(*TestOpts).BenchOpts.TenantConnString != "" {
		tenantCacheDBOpts.ConnString = b.TestOpts.(*TestOpts).BenchOpts.TenantConnString
	}

	var tenantCacheDatabase, err = NewDBConnector(&tenantCacheDBOpts, -1, true, b.Logger, 1)
	if err != nil {
		b.Exit("db: cannot create tenants cache connection: %v", err)
		return
	}

	if err = b.Vault.(*DBTestData).TenantsCache.Init(tenantCacheDatabase.Database); err != nil {
		b.Exit("db: cannot initialize tenants cache: %v", err)
	}

	tenantCacheDatabase.Release()

	if b.TestOpts.(*TestOpts).BenchOpts.Events {
		var workingConn *DBConnector
		if workingConn, err = NewDBConnector(&b.TestOpts.(*TestOpts).DBOpts, -1, true, b.Logger, 1); err != nil {
			return
		}

		b.Vault.(*DBTestData).EventBus = events.NewEventBus(workingConn.Database, b.Logger)
		b.Vault.(*DBTestData).EventBus.CreateTables()
	}

	tableName := testDesc.Table.TableName
	if tableName == "" {
		testDesc.Table.RowsCount = 0
		return
	}

	var ddlConnDatabase *DBConnector
	if ddlConnDatabase, err = NewDBConnector(&tenantCacheDBOpts, -1, true, b.Logger, 1); err != nil {
		b.Exit("db: cannot create connection for DDL: %v", err)
		return
	}

	conn := ddlConnDatabase
	testData := b.Vault.(*DBTestData)
	testData.TestDesc = testDesc

	t := testRegistry.GetTableByName(tableName)

	b.Logger.Debug("initializing table '%s'", tableName)
	if testDesc.IsReadonly {
		t.Create(conn, b)
		b.Logger.Debug("readonly test, skipping table '%s' initialization", tableName)
		if exists, err := conn.Database.TableExists(tableName); err != nil {
			b.Exit(fmt.Sprintf("db: cannot check if table '%s' exists: %v", tableName, err))
		} else if !exists {
			b.Exit("The '%s' table doesn't exist, please create tables using -I option, or use individual insert test using the -t `insert-***`", tableName)
		}
	} else {
		b.Logger.Debug("creating table '%s'", tableName)
		t.Create(conn, b)
	}

	var session = conn.Database.Session(conn.Database.Context(context.Background(), false))
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

	testDesc.Table.RowsCount = uint64(rowNum)
	b.Logger.Debug("table '%s' has %d rows", tableName, testDesc.Table.RowsCount)

	if rowsRequired > 0 {
		if testDesc.Table.RowsCount < rowsRequired {
			b.Exit(fmt.Sprintf("table '%s' has %d rows, but this test requires at least %d rows, please insert it first and then re-run the test",
				testDesc.Table.TableName, testDesc.Table.RowsCount, rowsRequired))
		}
	}

	ddlConnDatabase.Release()

	if b.TestOpts.(*TestOpts).BenchOpts.ParquetDataSource != "" {
		var offset int64
		if !testDesc.IsReadonly {
			offset = rowNum
		}

		if err = NewParquetFileDataSourceForRandomizer(b, b.TestOpts.(*TestOpts).BenchOpts.ParquetDataSource, offset); err != nil {
			b.Exit("failed to create parquet data source: %v", err)
		}
	}
}

func initWorker(worker *benchmark.BenchmarkWorker) {
	b := worker.Benchmark
	workerID := worker.WorkerID

	if worker.Data == nil {
		var workerData DBWorkerData
		var err error

		if workerData.workingConn, err = NewDBConnector(&b.TestOpts.(*TestOpts).DBOpts, workerID, false, worker.Logger, 1); err != nil {
			return
		}

		worker.Data = &workerData
	}

	worker.Logger.Trace("worker is initialized")
}

func initCommon(b *benchmark.Benchmark, testDesc *TestDesc, rowsRequired uint64) {
	b.Init = func() {
		initGeneric(b, testDesc, rowsRequired)
	}

	b.WorkerInitFunc = func(worker *benchmark.BenchmarkWorker) {
		initWorker(worker)
	}

	b.Metric = func() (metric string) {
		return testDesc.Metric
	}

	b.WorkerFinishFunc = func(worker *benchmark.BenchmarkWorker) {
		if workerData, ok := worker.Data.(*DBWorkerData); ok {
			workerData.release()
		}
	}

	b.PreExit = func() {
		if b.Vault != nil {
			if testData, ok := b.Vault.(*DBTestData); ok {
				// First stop the event bus if it exists
				if testData.EventBus != nil {
					testData.EventBus.Stop()
				}

				// Shutdown the connection pool first
				connPool.shutdown()

				// Then cleanup worker data
				for _, worker := range b.Workers {
					if workerData, ok := worker.Data.(*DBWorkerData); ok {
						if workerData.workingConn != nil {
							workerData.workingConn.Close()
						}
					}
				}
			}
		}
	}
}

/*
 * SELECT workers
 */

func TestGeneric(b *benchmark.Benchmark, testDesc *TestDesc, workerFunc TestWorkerFunc, rowsRequired uint64) {
	initCommon(b, testDesc, rowsRequired)

	b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
		c := worker.Data.(*DBWorkerData).workingConn
		batch := b.Vault.(*DBTestData).EffectiveBatch

		return workerFunc(b, c, testDesc, batch)
	}

	b.Run()

	b.Vault.(*DBTestData).Scores[testDesc.Category] = append(b.Vault.(*DBTestData).Scores[testDesc.Category], b.Score)
}

func TestSelectRun(
	b *benchmark.Benchmark,
	testDesc *TestDesc,
	fromFunc func(worker *benchmark.BenchmarkWorker) string,
	what []string,
	variablesToRead []interface{},
	whereFunc func(worker *benchmark.BenchmarkWorker) map[string][]string,
	orderByFunc func(worker *benchmark.BenchmarkWorker) []string,
	rowsRequired uint64,
) {
	initCommon(b, testDesc, rowsRequired)
	testOpts, ok := b.TestOpts.(*TestOpts)
	if !ok {
		b.Exit("TestOpts type conversion error")
	}

	explain := testOpts.DBOpts.Explain

	batch := b.Vault.(*DBTestData).EffectiveBatch

	type row struct {
		ID int64 `db:"id"`
	}

	b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
		b := worker.Benchmark
		c := worker.Data.(*DBWorkerData).workingConn

		from := testDesc.Table.TableName
		if fromFunc != nil {
			from = fromFunc(worker)
		}

		var whereCond map[string][]string
		if whereFunc != nil {
			whereCond = whereFunc(worker)
		}

		var orderBy []string
		if orderByFunc != nil {
			orderBy = orderByFunc(worker)
		}

		if testDesc.IsDBRTest {
			if rawSession, casted := c.Database.RawSession().(*dbr.Session); casted {
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

		var session = c.Database.Session(c.Database.Context(context.Background(), explain))
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
			if scanErr := rows.Scan(variablesToRead...); scanErr != nil {
				// b.Exit(scanErr)
			}
		}

		rows.Close()

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).Scores[testDesc.Category] = append(b.Vault.(*DBTestData).Scores[testDesc.Category], b.Score)
}

func TestSelectRawSQLQuery(
	b *benchmark.Benchmark,
	testDesc *TestDesc,
	fromFunc func(worker *benchmark.BenchmarkWorker) string,
	what string,
	whereFunc func(worker *benchmark.BenchmarkWorker) string,
	orderByFunc func(worker *benchmark.BenchmarkWorker) string,
	rowsRequired uint64,
) {
	initCommon(b, testDesc, rowsRequired)
	batch := b.Vault.(*DBTestData).EffectiveBatch

	b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
		c := worker.Data.(*DBWorkerData).workingConn

		from := testDesc.Table.TableName
		if fromFunc != nil {
			from = fromFunc(worker)
		}
		where := ""
		if whereFunc != nil {
			where = whereFunc(worker)
		}
		orderBy := ""
		if orderByFunc != nil {
			orderBy = orderByFunc(worker)
		}

		var dialectName = c.Database.DialectName()

		var query string
		switch dialectName {
		case db.MSSQL:
			query = fmt.Sprintf("SELECT {LIMIT} %s FROM %s {WHERE} {ORDERBY}", what, from)
		default:
			query = fmt.Sprintf("SELECT %s FROM %s {WHERE} {ORDERBY} {LIMIT}", what, from)
		}

		if where == "" {
			query = strings.Replace(query, "{WHERE}", "", -1)
		} else {
			query = strings.Replace(query, "{WHERE}", fmt.Sprintf("WHERE %s", where), -1) //nolint:perfsprint
		}

		if batch == 0 {
			query = strings.Replace(query, "{LIMIT}", "", -1)
		} else {
			switch dialectName {
			case db.MSSQL:
				query = strings.Replace(query, "{LIMIT}", fmt.Sprintf("TOP %d", batch), -1)
			default:
				query = strings.Replace(query, "{LIMIT}", fmt.Sprintf("LIMIT %d", batch), -1)
			}
		}

		if orderBy == "" {
			query = strings.Replace(query, "{ORDERBY}", "", -1)
		} else {
			query = strings.Replace(query, "{ORDERBY}", fmt.Sprintf("ORDER BY %s", orderBy), -1) //nolint:perfsprint
		}

		if dialectName == db.MYSQL || dialectName == db.SQLITE || dialectName == db.CASSANDRA {
			query = regexp.MustCompile(`\$\d+`).ReplaceAllString(query, "?")
		}

		var explain = b.TestOpts.(*TestOpts).DBOpts.Explain
		var session = c.Database.Session(c.Database.Context(context.Background(), explain))
		var rows, err = session.Query(query)
		if err != nil {
			b.Exit("db: cannot select rows: %v", err)
		}

		for rows.Next() {
			if err != nil {
				b.Exit(err)
			}
		}

		rows.Close()

		return batch
	}

	b.Run()

	b.Vault.(*DBTestData).Scores[testDesc.Category] = append(b.Vault.(*DBTestData).Scores[testDesc.Category], b.Score)
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

func TestInsertGeneric(b *benchmark.Benchmark, testDesc *TestDesc) {
	colConfs := testDesc.Table.GetColumnsForInsert(db.WithAutoInc(getDBDriver(b)))

	if len(*colConfs) == 0 {
		b.Exit(fmt.Sprintf("internal error: no columns eligible for INSERT found in '%s' configuration", testDesc.Table.TableName))
	}

	initCommon(b, testDesc, 0)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.Table

	var dialectName, dialErr = db.GetDialectName(b.TestOpts.(*TestOpts).DBOpts.ConnString)
	if dialErr != nil {
		b.Exit(dialErr)
	}

	if dialectName == db.CLICKHOUSE {
		sql := fmt.Sprintf("INSERT INTO %s", table.TableName) //nolint:perfsprint

		b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
			workerData := worker.Data.(*DBWorkerData)
			rows := table.RowsCount

			var c = workerData.workingConn
			var sess = c.Database.Session(c.Database.Context(context.Background(), false))

			if txErr := sess.Transact(func(tx db.DatabaseAccessor) error {
				var txBatch, prepareErr = tx.Prepare(sql)
				if prepareErr != nil {
					return prepareErr
				}

				for i := 0; i < batch; i++ {
					// clickhouse doesn't support autoincremented ID, so need to maintain it here
					_, values, err := worker.Randomizer.GenFakeData(colConfs, false)
					if err != nil {
						return err
					}
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

					if worker.Logger.GetLevel() >= logger.LevelDebug {
						worker.Logger.Debug(fmt.Sprintf("%s %v", sql, args))
					}
				}

				defer txBatch.Close()

				return nil
			}); txErr != nil {
				b.Exit(txErr.Error())
			}

			return batch
		}
	} else if testDesc.IsDBRTest {
		b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
			var t time.Time
			if worker.Logger.GetLevel() >= logger.LevelDebug {
				t = time.Now()
			}

			c := worker.Data.(*DBWorkerData).workingConn

			var rawDbrSess = c.Database.RawSession()
			var dbrSess = rawDbrSess.(*dbr.Session)

			tx, err := dbrSess.Begin()
			worker.Logger.Debug("BEGIN")
			if err != nil {
				worker.Exit(err)
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				columns, values, err := worker.Randomizer.GenFakeData(colConfs, false)
				if err != nil {
					b.Exit(err)
				}
				_, err = tx.InsertInto(table.TableName).Columns(columns...).Values(values...).Exec()
				if err != nil {
					b.Exit(err)
				}
			}

			err = tx.Commit()
			if err != nil {
				b.Exit("Commit() error: %s", err)
			}

			if worker.Logger.GetLevel() >= logger.LevelDebug {
				worker.Logger.Debug(fmt.Sprintf("COMMIT # dur: %.6f", time.Since(t).Seconds()))
			}

			return batch
		}
	} else {
		b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
			workerData := worker.Data.(*DBWorkerData)

			var c = workerData.workingConn
			var sess = c.Database.Session(c.Database.Context(context.Background(), false))

			if txErr := sess.Transact(func(tx db.DatabaseAccessor) error {
				for i := 0; i < batch; i++ {
					columns, values, err := worker.Randomizer.GenFakeData(colConfs, false)
					if err != nil {
						return err
					}

					if err := tx.BulkInsert(table.TableName, [][]interface{}{values}, columns); err != nil {
						return err
					}

					if b.TestOpts.(*TestOpts).BenchOpts.Events {
						rw := worker.Randomizer
						if err := b.Vault.(*DBTestData).EventBus.InsertEvent(rw, tx, rw.UUID()); err != nil {
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

	b.Vault.(*DBTestData).Scores[testDesc.Category] = append(b.Vault.(*DBTestData).Scores[testDesc.Category], b.Score)
}

// InsertMultiValueDataWorker inserts a row into the 'light' table using INSERT INTO t (x, y, z) VALUES (..., ..., ...)
func InsertMultiValueDataWorker(b *benchmark.Benchmark, c *DBConnector, testDesc *TestDesc, batch int) (loops int) {
	colConfs := testDesc.Table.GetColumnsForInsert(db.WithAutoInc(c.Database.DialectName()))

	var columns []string
	var values [][]interface{}
	for i := 0; i < batch; i++ {
		var genColumns, vals, err = b.Randomizer.GenFakeData(colConfs, db.WithAutoInc(c.Database.DialectName()))
		if err != nil {
			b.Exit(err)
		}

		if genColumns == nil {
			break
		}

		values = append(values, vals)
		if i == 0 {
			columns = genColumns
		}
	}

	if len(values) == 0 {
		return
	}

	var session = c.Database.Session(c.Database.Context(context.Background(), false))
	if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
		return tx.BulkInsert(testDesc.Table.TableName, values, columns)
	}); txErr != nil {
		b.Exit(txErr.Error())
	}

	return batch
}

/*
 * UPDATE worker
 */

func TestUpdateGeneric(b *benchmark.Benchmark, testDesc *TestDesc, updateRows uint64, colConfs *[]benchmark.DBFakeColumnConf) {
	if colConfs == nil {
		colConfs = testDesc.Table.GetColumnsForUpdate(db.WithAutoInc(getDBDriver(b)))
	}

	if len(*colConfs) == 0 {
		b.Exit(fmt.Sprintf("internal error: no columns eligible for UPDATE found in '%s' configuration", testDesc.Table.TableName))
	}

	initCommon(b, testDesc, updateRows)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.Table

	if testDesc.IsDBRTest {
		b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
			var t time.Time
			if worker.Logger.GetLevel() >= logger.LevelDebug {
				t = time.Now()
			}

			c := worker.Data.(*DBWorkerData).workingConn

			var rawDbrSess = c.Database.RawSession()
			var dbrSess = rawDbrSess.(*dbr.Session)

			tx, err := dbrSess.Begin()
			worker.Logger.Debug("BEGIN")
			if err != nil {
				worker.Exit(err)
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				columns, err := worker.Randomizer.GenFakeDataAsMap(colConfs, false)
				if err != nil {
					worker.Exit(err)
				}
				id := int64(worker.Randomizer.Uintn64(table.RowsCount - updateRows))

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

			if worker.Logger.GetLevel() >= logger.LevelDebug {
				worker.Logger.Debug(fmt.Sprintf("COMMIT # dur: %.6f", time.Since(t).Seconds()))
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
		updateSQL := FormatSQL(updateSQLTemplate, dialectName)

		b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
			var c = worker.Data.(*DBWorkerData).workingConn
			var session = c.Database.Session(c.Database.Context(context.Background(), false))
			if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
				for i := 0; i < batch; i++ {
					id := int64(worker.Randomizer.Uintn64(table.RowsCount-updateRows) + updateRows)
					_, fakeDataValues, err := worker.Randomizer.GenFakeData(colConfs, false)
					if err != nil {
						return err
					}

					fakeDataValues = append(fakeDataValues, id)
					if updateRows > 1 {
						fakeDataValues = append(fakeDataValues, id-int64(updateRows))
					}

					if _, err = tx.Exec(updateSQL, fakeDataValues...); err != nil {
						return err
					}

					if b.TestOpts.(*TestOpts).BenchOpts.Events {
						rw := worker.Randomizer
						if err = b.Vault.(*DBTestData).EventBus.InsertEvent(rw, tx, rw.UUID()); err != nil {
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

	b.Vault.(*DBTestData).Scores[testDesc.Category] = append(b.Vault.(*DBTestData).Scores[testDesc.Category], b.Score)
}

/*
 * DELETE worker
 */
// testDeleteGeneric is a generic DELETE worker
func testDeleteGeneric(b *benchmark.Benchmark, testDesc *TestDesc, deleteRows uint64) { //nolint:unused
	initCommon(b, testDesc, deleteRows)

	batch := b.Vault.(*DBTestData).EffectiveBatch
	table := &testDesc.Table

	var dialectName, err = db.GetDialectName(b.TestOpts.(*TestOpts).DBOpts.ConnString)
	if err != nil {
		b.Exit(err)
	}

	if testDesc.IsDBRTest {
		b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
			var t time.Time
			if worker.Logger.GetLevel() >= logger.LevelDebug {
				t = time.Now()
			}

			c := worker.Data.(*DBWorkerData).workingConn

			var rawDbrSess = c.Database.RawSession()
			var dbrSess = rawDbrSess.(*dbr.Session)

			tx, err := dbrSess.Begin()
			worker.Logger.Debug("BEGIN")
			if err != nil {
				b.Exit(err)
			}
			defer tx.RollbackUnlessCommitted() // Rollback in case of error

			for i := 0; i < batch; i++ {
				id := int64(worker.Randomizer.Uintn64(table.RowsCount - deleteRows))

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
				b.Exit(err)
			}

			if worker.Logger.GetLevel() >= logger.LevelDebug {
				worker.Logger.Debug(fmt.Sprintf("COMMIT # dur: %.6f", time.Since(t).Seconds()))
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
		deleteSQL := FormatSQL(deleteSQLTemplate, dialectName)

		b.WorkerRunFunc = func(worker *benchmark.BenchmarkWorker) (loops int) {
			var c = worker.Data.(*DBWorkerData).workingConn
			var session = c.Database.Session(c.Database.Context(context.Background(), false))
			if txErr := session.Transact(func(tx db.DatabaseAccessor) error {
				for i := 0; i < batch; i++ {
					id := int64(worker.Randomizer.Uintn64(table.RowsCount-deleteRows) + deleteRows)
					var values []interface{}

					values = append(values, id)
					if deleteRows > 1 {
						values = append(values, id-int64(deleteRows))
					}

					if _, err := tx.Query(deleteSQL, values...); err != nil {
						return err
					}

					if b.TestOpts.(*TestOpts).BenchOpts.Events {
						rw := worker.Randomizer
						if err := b.Vault.(*DBTestData).EventBus.InsertEvent(rw, tx, rw.UUID()); err != nil {
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

	b.Vault.(*DBTestData).Scores[testDesc.Category] = append(b.Vault.(*DBTestData).Scores[testDesc.Category], b.Score)
}
