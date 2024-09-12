package sql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/acronis/perfkit/db"
)

type sqlDialect struct {
	dia dialect
}

func (d *sqlDialect) GetType(dataType db.DataType) string {
	return d.dia.getType(dataType)
}

// applyMigrations applies a set of migrations to a table
func applyMigrations(q querier, d dialect, tableName, tableMigrationSQL string) error {
	var migrationQueries []string

	tableMigrationSQL, err := db.DefaultCreateQueryPatchFunc(tableName, tableMigrationSQL, &sqlDialect{dia: d})
	if err != nil {
		return fmt.Errorf("error applying default create query patch: %v", err)
	}

	switch d.name() {
	case db.MYSQL:
		// Percona (or MySQL?) fails to create all the steps within single transaction
		migrationQueries = strings.Split(tableMigrationSQL, ";")
	case db.CASSANDRA:
		migrationQueries = strings.Split(tableMigrationSQL, ";")
	default:
		migrationQueries = []string{tableMigrationSQL}
	}

	for i := range migrationQueries {
		query := strings.TrimSpace(migrationQueries[i])
		if query != "" {
			_, err = q.execContext(context.Background(), query)
			if err != nil {
				return fmt.Errorf("DB migration failed: %s, error: %s", query, err.Error())
			}
		}
	}

	return nil
}

// tableExists checks if a table exists
func tableExists(q querier, d dialect, name string) (bool, error) {
	var query string
	var args = []interface{}{name}

	switch d.name() {
	case db.SQLITE:
		if name == "sqlite_master" {
			return true, nil
		}

		query = `
			SELECT count(*)
			FROM sqlite_master
			WHERE name = '%v'
			  AND type = 'table';`

	case db.MYSQL:
		if name == "information_schema" {
			return true, nil
		}

		query = `
			SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_name = '%v'
			  AND table_schema = DATABASE();
			`

	case db.POSTGRES:
		if name == "information_schema" {
			return true, nil
		}

		query = `
			SELECT COUNT(*)
			FROM information_schema.tables
			WHERE table_type LIKE 'BASE TABLE' 
			  AND table_name = '%v'
			`
		if d.schema() != "" {
			query += " AND table_schema LIKE '%v';"
			args = append(args, d.schema())
		}

	case db.MSSQL:
		if name == "INFORMATION_SCHEMA" {
			return true, nil
		}

		query = `
			IF EXISTS(SELECT *
					  FROM INFORMATION_SCHEMA.TABLES
					  WHERE TABLE_TYPE = 'BASE TABLE'
						AND TABLE_NAME = '%v')
				SELECT 1 AS res
			ELSE
				SELECT 0 AS res;
			`

	case db.CASSANDRA:
		if name == "system_schema" {
			return true, nil
		}

		query = `
			SELECT count(*) 
			FROM system_schema.tables 
			WHERE table_name = '%v'
			`
		if d.schema() != "" {
			query += " AND keyspace_name = '%v'"
			args = append(args, d.schema())
		}

		query += " ALLOW FILTERING;"

	case db.CLICKHOUSE:
		if name == "system" {
			return true, nil
		}

		query = `
			SELECT COUNT() 
			FROM system.tables 
			WHERE name = '%v';`

	default:
		return false, fmt.Errorf("unsupported driver: %s", d.name())
	}

	var check = fmt.Sprintf(query, args...)
	var exists int
	if err := q.queryRowContext(context.Background(), check).Scan(&exists); err != nil && err != sql.ErrNoRows {
		return false, err
	}

	return exists != 0, nil
}

func constructSQLDDLQuery(d dialect, tableName string, tableDefinition *db.TableDefinition) string {
	if tableDefinition == nil {
		return ""
	}

	var query = fmt.Sprintf("CREATE TABLE %v (", d.table(tableName))
	for i, row := range tableDefinition.TableRows {
		query += fmt.Sprintf("%v %v", row.Name, d.getType(row.Type))
		if row.NotNull {
			if d.name() != db.CASSANDRA {
				query += " NOT NULL"
			}
		}
		if i < len(tableDefinition.TableRows)-1 {
			query += ", "
		}
	}

	if len(tableDefinition.PrimaryKey) != 0 {
		query += ", PRIMARY KEY ("
		for i, key := range tableDefinition.PrimaryKey {
			query += key
			if i < len(tableDefinition.PrimaryKey)-1 {
				query += ", "
			}
		}
		query += ")"
	}

	if tableDefinition.Engine != "" {
		query += ") ENGINE = " + tableDefinition.Engine
	} else {
		query += ")"
	}

	return query
}

// createTable creates a table if it doesn't exist
func createTable(q querier, d dialect, name string, tableDefinition *db.TableDefinition, ddlQuery string) error {
	if name == "" {
		return nil
	}

	if tableDefinition != nil {
		if err := createSelectQueryBuilder(name, tableDefinition.TableRows); err != nil {
			return err
		}
	}

	if exists, err := tableExists(q, d, name); err != nil {
		return fmt.Errorf("error checking table existence: %v", err)
	} else if exists {
		return nil
	}

	if tableDefinition != nil {
		ddlQuery = constructSQLDDLQuery(d, name, tableDefinition)
	}

	if ddlQuery == "" {
		return fmt.Errorf("internal error: table %s needs to be created, but migration query has not been provided", name)
	}

	if err := applyMigrations(q, d, name, ddlQuery); err != nil {
		return fmt.Errorf("error applying migrations: %v", err)
	}

	return nil
}

// dropTable drops a table if it exists
func dropTable(q querier, d dialect, name string, useTruncate bool) error {
	var query string
	var args = []interface{}{name}

	if useTruncate {
		if exists, err := tableExists(q, d, name); err != nil {
			return fmt.Errorf("error checking table existence: %v", err)
		} else if exists {
			switch d.name() {
			case db.POSTGRES:
				query = "TRUNCATE TABLE %v CASCADE"
			default:
				query = "TRUNCATE TABLE %v"
			}
		}
	} else {
		query = "DROP TABLE IF EXISTS %v"
	}

	var drop = fmt.Sprintf(query, args...)
	if _, err := q.execContext(context.Background(), drop); err != nil {
		return err
	}

	return nil
}

// indexExists checks if an index exists
func indexExists(q querier, d dialect, indexName, tableName string) (bool, error) {
	var qry string
	var args = []interface{}{tableName, indexName}

	switch d.name() {
	case db.SQLITE:
		qry = `
			SELECT count(*)
			FROM sqlite_master
			WHERE tbl_name = '%v'
			  AND name = '%v'
			  AND type = 'index';
			`

	case db.MYSQL:
		qry = `
			SELECT COUNT(*)
			FROM information_schema.statistics
			WHERE table_name = '%v'
			  AND index_name = '%v'
			  AND table_schema = DATABASE();
			`

	case db.POSTGRES:
		qry = `
			SELECT COUNT(*)
			FROM pg_index ix
					 JOIN pg_class t on t.oid = ix.indrelid
					 JOIN pg_class i on i.oid = ix.indexrelid
					 JOIN pg_namespace n on n.oid = t.relnamespace AND n.oid = i.relnamespace
			WHERE t.relkind = 'r'
			  AND t.relname = '%v'
			  AND i.relname = '%v'
			`
		if d.schema() != "" {
			qry += " AND n.nspname = '%v'"
			args = append(args, d.schema())
		}

	case db.MSSQL:
		qry = `
			SELECT count(*)
			FROM sys.indexes
			WHERE object_id = OBJECT_ID('%v')
			  AND name = '%v';
			`

	case db.CASSANDRA:
		return true, nil
	case db.CLICKHOUSE:
		// CLICKHOUSE don't require to create indexes
		return true, nil
	}

	var check = fmt.Sprintf(qry, args...)
	var exists int
	if err := q.queryRowContext(context.Background(), check).Scan(&exists); err != nil {
		return false, err
	}

	return exists != 0, nil
}

// makeIndexName returns an index name for a given table and columns
func makeIndexName(tableName string, columns string, id int) string {
	name := strings.Split(columns, " ")[0]
	name = strings.ReplaceAll(name, ",", "")

	return fmt.Sprintf("%s_idx_%s_%d", tableName, name, id)
}

// createIndex creates an index if it doesn't exist for a given table and columns
func createIndex(q querier, d dialect, indexName string, tableName string, columns []string, indexType db.IndexType) error {
	if tableName == "" || len(columns) == 0 {
		return nil
	}

	if exists, err := indexExists(q, d, indexName, tableName); err != nil {
		return fmt.Errorf("error checking index existence: %v", err)
	} else if exists {
		return nil
	}

	var qry = fmt.Sprintf("CREATE INDEX %v ON %v (%v)", indexName, d.table(tableName), strings.Join(columns, ", "))
	var _, err = q.execContext(context.Background(), qry)

	return err
}

// dropIndex drops an index if it exists
func dropIndex(q querier, d dialect, indexName, tableName string) error {
	if exists, err := indexExists(q, d, indexName, tableName); err != nil {
		return fmt.Errorf("db: cannot check index '%v' existence, error: %v", indexName, err)
	} else if !exists {
		return nil
	}

	var qry string
	switch d.name() {
	case db.SQLITE:
		qry = fmt.Sprintf("DROP INDEX %v;", indexName)
	case db.POSTGRES:
		if d.schema() != "" {
			qry = fmt.Sprintf("DROP INDEX %v.%v;", d.schema(), indexName)
		} else {
			qry = fmt.Sprintf("DROP INDEX %v;", indexName)
		}
	case db.CLICKHOUSE:
		return nil
	default:
		qry = fmt.Sprintf("DROP INDEX %v ON %v;", indexName, d.table(tableName))
	}
	var _, err = q.execContext(context.Background(), qry)
	return err
}

// readConstraints reads constraints from the database
func readConstraints(q querier, d dialect) ([]db.Constraint, error) {
	if d.name() != db.POSTGRES {
		return nil, nil
	}

	query := `
	SELECT conname, conrelid::regclass AS table_name, pg_get_constraintdef(oid) AS condef
	FROM pg_constraint
	WHERE contype IN ('f', 'p', 'u');
	`

	var rows, err = q.queryContext(context.Background(), query)
	if err != nil {
		return nil, fmt.Errorf("error querying constraints: %v", err)
	}

	defer rows.Close()

	var constraints []db.Constraint

	for rows.Next() {
		var constraint db.Constraint
		if err = rows.Scan(&constraint.Name, &constraint.TableName, &constraint.Definition); err != nil {
			return nil, fmt.Errorf("error scanning row: %v", err)
		}

		if !strings.HasPrefix(constraint.TableName, "acronis_db_bench") {
			continue
		}

		if strings.Contains(strings.ToLower(constraint.Definition), "foreign key") {
			constraints = append(constraints, constraint)
		}
	}

	return constraints, nil
}

// addConstraints restores constraints in the database
func addConstraints(q querier, d dialect, constraints []db.Constraint) error {
	if d.name() != db.POSTGRES || constraints == nil {
		return nil
	}

	for _, constraint := range constraints {
		query := fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s %s", constraint.TableName, constraint.Name, constraint.Definition)

		_, err := q.execContext(context.Background(), query)
		if err != nil {
			return err
		}
	}

	return nil
}

// dropConstraints removes constraints from the database
func dropConstraints(q querier, d dialect, constraints []db.Constraint) error {
	if d.name() != db.POSTGRES || constraints == nil {
		return nil
	}

	for _, constraint := range constraints {
		query := fmt.Sprintf("ALTER TABLE %s DROP CONSTRAINT %s", constraint.TableName, constraint.Name)

		_, err := q.execContext(context.Background(), query)
		if err != nil {
			return err
		}
	}

	return nil
}

// createSequence creates a sequence if it doesn't exist
func createSequence(q querier, d dialect, sequenceName string) error {
	switch d.name() {
	case db.SQLITE:
		if exists, err := tableExists(q, d, sequenceName); err != nil {
			return fmt.Errorf("error checking table existence: %v", err)
		} else if !exists {
			if err = createTable(q, d, sequenceName, nil, fmt.Sprintf(`
				CREATE TABLE %s (value BIGINT NOT NULL, sequence_id INT NOT NULL); 
				CREATE INDEX %s_value ON %s (value);
				`,
				sequenceName, sequenceName, sequenceName)); err != nil {
				return err
			}
			_, err = q.execContext(context.Background(), fmt.Sprintf("INSERT INTO %s (value, sequence_id) VALUES (1, 1)", sequenceName))
			return err
		}
	case db.MYSQL, db.POSTGRES:
		_, err := q.execContext(context.Background(), fmt.Sprintf("CREATE SEQUENCE IF NOT EXISTS %v", sequenceName))
		return err
	case db.MSSQL:
		_, err := q.execContext(context.Background(), fmt.Sprintf(`
			IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = '%[1]s') BEGIN CREATE SEQUENCE %[1]s AS BIGINT START WITH 1 INCREMENT BY 1; END;
			`, sequenceName))
		return err
	case db.CLICKHOUSE, db.CASSANDRA:
		// CLICKHOUSE and CASSANDRA can't manage sequences
	default:
		return fmt.Errorf("unsupported driver: %s", d.name())
	}

	return nil
}

// DropSequence drops a sequence if it exists
func dropSequence(q querier, d dialect, sequenceName string) error {
	switch d.name() {
	case db.SQLITE:
		return dropTable(q, d, sequenceName, false)
	case db.MYSQL, db.POSTGRES, db.MSSQL:
		_, err := q.execContext(context.Background(), fmt.Sprintf("DROP SEQUENCE IF EXISTS %v", sequenceName))
		return err
	case db.CLICKHOUSE, db.CASSANDRA:
		//
	default:
		return fmt.Errorf("unsupported driver: %s", d.name())
	}

	return nil
}

// getTableMigrationSQL returns a table migration query for a given driver
func getTableMigrationSQL(tableMigrationSQL string, dialect db.DialectName, engine string) (string, error) { //nolint:unused
	switch dialect {
	case db.SQLITE, db.SQLITE3:
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeId), "id INTEGER PRIMARY KEY")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "$binaryblobtype", "MEDIUMBLOB")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeEngine), "")
	case db.MYSQL:
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeId), "id bigint not null AUTO_INCREMENT PRIMARY KEY")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "$binaryblobtype", "MEDIUMBLOB")
		if engine == "xpand-allnodes" {
			tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeEngine), "engine = xpand")
		} else {
			tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeEngine), "engine = "+engine)
		}
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$json_type}", "json")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$json_index}",
			"ALTER TABLE acronis_db_bench_json ADD COLUMN _data_f0f0 VARCHAR(1024) AS (JSON_EXTRACT(json_data, '$.field0.field0')) STORED;"+
				"ALTER TABLE acronis_db_bench_json ADD COLUMN _data_f0f0f0 VARCHAR(1024) AS (JSON_EXTRACT(json_data, '$.field0.field0.field0')) STORED;"+
				"CREATE INDEX acronis_db_bench_json_idx_data_f0f0 ON acronis_db_bench_json(_data_f0f0);"+
				"CREATE INDEX acronis_db_bench_json_idx_data_f0f0f0 ON acronis_db_bench_json(_data_f0f0f0);")
	case db.MSSQL:
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeId), "id bigint IDENTITY(1,1) PRIMARY KEY")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "$binaryblobtype", "varbinary(max)")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeEngine), "")
	case db.POSTGRES:
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeId), "id bigserial not null PRIMARY KEY")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "$binaryblobtype", "BYTEA")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, string(db.DataTypeEngine), "")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$json_type}", "jsonb")
		tableMigrationSQL = strings.ReplaceAll(tableMigrationSQL, "{$json_index}",
			"CREATE INDEX acronis_db_bench_json_idx_data ON acronis_db_bench_json USING GIN (json_data jsonb_path_ops)")
	default:
		return "", fmt.Errorf("unsupported driver: '%v', supported drivers are: postgres|sqlite|mysql|mssql", dialect)
	}

	return tableMigrationSQL, nil
}
