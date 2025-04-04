package basic_scenarios

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/lib/pq"

	"github.com/acronis/perfkit/benchmark"
	"github.com/acronis/perfkit/db"

	"github.com/acronis/perfkit/acronis-db-bench/engine"

	tenants "github.com/acronis/perfkit/acronis-db-bench/tenants-cache"
)

// insertByPreparedDataWorker inserts a row into the 'light' table using prepared statement for the batch
func insertByPreparedDataWorker(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) {
	colConfs := testDesc.Table.GetColumnsForInsert(db.WithAutoInc(c.Database.DialectName()))
	sess := c.Database.Session(c.Database.Context(context.Background(), false))

	if txErr := sess.Transact(func(tx db.DatabaseAccessor) error {
		columns, _, err := b.Randomizer.GenFakeData(colConfs, false)
		if err != nil {
			b.Exit(err)
		}

		parametersPlaceholder := db.GenDBParameterPlaceholders(0, len(*colConfs))
		sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES(%s)", testDesc.Table.TableName, strings.Join(columns, ","), parametersPlaceholder)
		sql = engine.FormatSQL(sql, c.Database.DialectName())

		stmt, err := tx.Prepare(sql)

		if err != nil {
			c.Exit(err.Error())
		}
		for i := 0; i < batch; i++ {
			_, values, err := b.Randomizer.GenFakeData(colConfs, false)
			if err != nil {
				b.Exit(err)
			}

			_, err = stmt.Exec(values...)

			if err != nil {
				stmt.Close()
				c.Exit(err.Error())
			}
		}

		return nil
	}); txErr != nil {
		c.Exit(txErr.Error())
	}

	return batch
}

// copyDataWorker copies a row into the 'light' table
func copyDataWorker(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, batch int) (loops int) {
	var sql string
	colConfs := testDesc.Table.GetColumnsForInsert(db.WithAutoInc(c.Database.DialectName()))
	sess := c.Database.Session(c.Database.Context(context.Background(), false))

	if txErr := sess.Transact(func(tx db.DatabaseAccessor) error {
		columns, _, err := b.Randomizer.GenFakeData(colConfs, false)
		if err != nil {
			b.Exit(err)
		}

		switch c.Database.DialectName() {
		case db.POSTGRES:
			sql = pq.CopyIn(testDesc.Table.TableName, columns...)
		case db.MSSQL:
			sql = mssql.CopyIn(testDesc.Table.TableName, mssql.BulkOptions{KeepNulls: true, RowsPerBatch: batch}, columns...)
		default:
			b.Exit("unsupported driver: '%v', supported drivers are: %s|%s", c.Database.DialectName(), db.POSTGRES, db.MSSQL)
		}

		stmt, err := tx.Prepare(sql)

		if err != nil {
			c.Exit(err.Error())
		}
		for i := 0; i < batch; i++ {
			_, values, err := b.Randomizer.GenFakeData(colConfs, false)
			if err != nil {
				b.Exit(err)
			}

			_, err = stmt.Exec(values...)

			if err != nil {
				stmt.Close()
				c.Exit(err.Error())
			}
		}

		_, err = stmt.Exec()
		if err != nil {
			stmt.Close()
			c.Exit(err.Error())
		}

		return nil
	}); txErr != nil {
		c.Exit(txErr.Error())
	}

	return batch
}

/*
 * Other
 */

func tenantAwareCTIAwareWorker(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, orderBy string, batch int) (loops int) { //nolint:revive
	c.Logger.Trace("tenant-aware and CTI-aware SELECT test iteration")

	tableName := testDesc.Table.TableName
	query := buildTenantAwareQuery(tableName)
	ctiUUID, err := b.Vault.(*engine.DBTestData).TenantsCache.GetRandomCTIUUID(b.Randomizer, 0)
	if err != nil {
		b.Exit(err)
	}
	ctiAwareQuery := query + fmt.Sprintf(
		" JOIN `%[1]s` AS `cti_ent` "+
			"ON `cti_ent`.`uuid` = `%[2]s`.`cti_entity_uuid` AND `%[2]s`.`cti_entity_uuid` IN ('%[4]s') "+
			"LEFT JOIN `%[3]s` as `cti_prov` "+
			"ON `cti_prov`.`tenant_id` = `tenants_child`.`id` AND `cti_prov`.`cti_entity_uuid` = `%[2]s`.`cti_entity_uuid` "+
			"WHERE `cti_prov`.`state` = 1 OR `cti_ent`.`global_state` = 1",
		tenants.TableNameCtiEntities, tableName, tenants.TableNameCtiProvisioning, string(ctiUUID))

	return tenantAwareGenericWorker(b, c, ctiAwareQuery, orderBy)
}

func tenantAwareWorker(b *benchmark.Benchmark, c *engine.DBConnector, testDesc *engine.TestDesc, orderBy string, batch int) (loops int) { //nolint:revive
	query := buildTenantAwareQuery(testDesc.Table.TableName)

	return tenantAwareGenericWorker(b, c, query, orderBy)
}

func buildTenantAwareQuery(tableName string) string {
	return fmt.Sprintf("SELECT `%[1]s`.`id` id, `%[1]s`.`tenant_id` FROM `%[1]s` "+
		"JOIN `%[2]s` AS `tenants_child` ON ((`tenants_child`.`uuid` = `%[1]s`.`tenant_id`) AND (`tenants_child`.`is_deleted` != {true})) "+
		"JOIN `%[3]s` AS `tenants_closure` ON ((`tenants_closure`.`child_id` = `tenants_child`.`id`) AND (`tenants_closure`.`barrier` <= 0)) "+
		"JOIN `%[2]s` AS `tenants_parent` ON ((`tenants_parent`.`id` = `tenants_closure`.`parent_id`) "+
		"AND (`tenants_parent`.`uuid` IN ('{tenant_uuid}')) AND (`tenants_parent`.`is_deleted` != {true}))",
		tableName, tenants.TableNameTenants, tenants.TableNameTenantClosure)
}

func tenantAwareGenericWorker(b *benchmark.Benchmark, c *engine.DBConnector, query string, orderBy string) (loops int) {
	c.Logger.Trace("tenant-aware SELECT test iteration")

	uuid, err := b.Vault.(*engine.DBTestData).TenantsCache.GetRandomTenantUUID(b.Randomizer, 0, "")
	if err != nil {
		b.Exit(err)
	}

	var valTrue string

	if c.Database.DialectName() == db.POSTGRES {
		valTrue = "true"
	} else {
		valTrue = "1"
	}
	query = strings.ReplaceAll(query, "{true}", valTrue)
	query = strings.ReplaceAll(query, "{tenant_uuid}", string(uuid))
	if orderBy != "" {
		query += " " + orderBy
	}
	query += " LIMIT 1"

	var id, tenantID string

	if c.Database.DialectName() == db.POSTGRES {
		query = strings.ReplaceAll(query, "`", "\"")
	}

	c.Logger.Trace("executing query: %s", query)

	var session = c.Database.Session(c.Database.Context(context.Background(), false))
	if err = session.QueryRow(query).Scan(&id, &tenantID); err != nil {
		if !errors.Is(sql.ErrNoRows, err) {
			c.Exit(err.Error())
		}
	}

	return 1
}
