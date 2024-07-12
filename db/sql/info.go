package sql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/acronis/perfkit/db"
)

// GetVersion returns DB version and driver name
func getVersion(q querier, d dialect) (db.DialectName, string, error) {
	var version string
	var query string

	switch d.name() {
	case db.POSTGRES:
		query = "SELECT version();"
	case db.MYSQL, db.CLICKHOUSE:
		query = "SELECT VERSION();"
	case db.CASSANDRA:
		query = "SELECT release_version FROM system.local;"
	case db.MSSQL:
		query = "SELECT @@VERSION;"
	case db.SQLITE:
		query = "SELECT sqlite_version();"
	default:
		return "", "", fmt.Errorf("unsupported driver: %s", d.name())
	}

	if err := q.QueryRowContext(context.Background(), query).Scan(&version); err != nil {
		return "", "", err
	}

	if d.name() == db.MYSQL {
		var versionComment string
		query = "SELECT @@VERSION_COMMENT;"

		if err := q.QueryRowContext(context.Background(), query).Scan(&versionComment); err != nil {
			return "", "", err
		}

		version = fmt.Sprintf("%s (%s)", version, versionComment)
	}

	return d.name(), version, nil
}

// GetInfo returns DB info
func getInfo(q querier, d dialect, version string) ([]string, *db.Info, error) {
	var ret []string
	var dbInfo = db.NewDBInfo(newSQLRecommendationsSource(q, d), version)

	switch d.name() {
	case db.POSTGRES:
		// Execute SHOW ALL command
		var rows, err = q.QueryContext(context.Background(), "SHOW ALL")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to execute query: %s, error: %s", "SHOW ALL", err)
		}
		defer rows.Close()

		header := "|-------------------------------------|--------------------------------------------------------------|------------|"

		ret = append(ret, header)
		ret = append(ret, fmt.Sprintf("| %-35s | %-60s | %-10s |", "Name", "Setting", "Unit"))
		ret = append(ret, header)

		for rows.Next() {
			var name, setting, unit sql.NullString
			if err = rows.Scan(&name, &setting, &unit); err != nil {
				return nil, nil, fmt.Errorf("failed to scan row: %s", err)
			}

			s := db.TernaryStr(name.Valid, name.String, "")
			v := db.TernaryStr(setting.Valid, setting.String, "")

			dbInfo.AddSetting(s, v)
			ret = append(ret, fmt.Sprintf("| %-35s | %-60s | %-10s |", s, v, db.TernaryStr(unit.Valid, setting.String, "")))
		}
		ret = append(ret, header)

		if err = rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error during row iteration: %s", err)
		}
	case db.MYSQL:
		query := "SHOW VARIABLES;"
		rows, err := q.QueryContext(context.Background(), query)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to execute query: %s, error: %s", query, err)
		}
		defer rows.Close()

		var variableName, value string

		header := "-----------------------------------------|-----------------------------------------------"
		ret = append(ret, header)
		ret = append(ret, fmt.Sprintf("%-40s | %-40s", "Variable_Name", "Value"))
		ret = append(ret, header)

		for rows.Next() {
			err = rows.Scan(&variableName, &value)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan row: %s", err)
			}
			dbInfo.AddSetting(variableName, value)
			ret = append(ret, fmt.Sprintf("%-40s | %-40s", variableName, value))
		}
		ret = append(ret, header)

		if err = rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error during row iteration: %s", err)
		}
	case db.MSSQL:
		query := "SELECT * FROM sys.configurations"
		rows, err := q.QueryContext(context.Background(), query)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to execute query: %s, error: %s", query, err)
		}
		defer rows.Close()

		cols, err := rows.Columns()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get columns: %s", err)
		}

		// Create a slice of interface{}'s to represent each column,
		// and a second slice to contain pointers to each item in the columns slice.
		values := make([]sql.RawBytes, len(cols))
		scanArgs := make([]interface{}, len(values))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		header := ""
		for range cols {
			header += strings.Repeat("-", 37)
		}
		ret = append(ret, header)

		str := ""
		for _, col := range cols {
			str += fmt.Sprintf("%-35s | ", col)
		}
		ret = append(ret, str)

		// Fetch rows
		for rows.Next() {
			err = rows.Scan(scanArgs...)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan row: %s", err)
			}

			var value string
			str = ""
			for _, col := range values {
				if col == nil {
					value = "NULL"
				} else {
					value = string(col)
				}
				str += fmt.Sprintf("%-35s | ", value)
			}
			ret = append(ret, str)
		}
		ret = append(ret, header)

		if err = rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error during row iteration: %s", err)
		}
	case db.CASSANDRA:
		// Execute a CQL query
		rows, err := q.QueryContext(context.Background(), "SELECT * FROM system.local") // Replace with your actual query
		if err != nil {
			return nil, nil, fmt.Errorf("failed to execute query: %s, error: %s", "SELECT * FROM system.local", err)
		}
		defer rows.Close()

		// Get column names
		columns, err := rows.Columns()
		if err != nil {
			return nil, nil, fmt.Errorf("failed to get columns: %s", err)
		}

		// Prepare a slice of interface{}'s to hold each value
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		// Iterate over rows
		for rows.Next() {
			err = rows.Scan(values...)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to scan row: %s", err)
			}

			for i, value := range values {
				ret = append(ret, fmt.Sprintf("%s: %v", columns[i], *(value.(*interface{}))))
			}
		}

		// Check for errors after iterating
		if err = rows.Err(); err != nil {
			return nil, nil, fmt.Errorf("error during row iteration: %s", err)
		}
	case db.SQLITE, db.CLICKHOUSE:
		//
	default:
		return nil, nil, fmt.Errorf("unsupported driver: %s", d.name())
	}

	return ret, dbInfo, nil
}

// GetTablesSchemaInfo returns the schema info for a given set of tables
func getTablesSchemaInfo(q querier, d dialect, tableNames []string) ([]string, error) {
	var ret []string

	for _, table := range tableNames {
		if exists, err := tableExists(q, d, table); err != nil {
			return nil, fmt.Errorf("error checking table existence: %w", err)
		} else if !exists {
			continue
		}

		ret = append(ret, fmt.Sprintf("TABLE: %s", table)) //nolint:perfsprint

		// Query to list columns

		var listColumnsQuery string
		switch d.name() {
		case db.POSTGRES, db.MYSQL, db.MSSQL:
			listColumnsQuery = fmt.Sprintf("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '%s'", table)
		case db.CLICKHOUSE:
			listColumnsQuery = fmt.Sprintf("SELECT name AS column_name, type AS data_type FROM system.columns WHERE table = '%s'", table)
		case db.CASSANDRA:
			listColumnsQuery = fmt.Sprintf("SELECT column_name, type FROM system_schema.columns WHERE keyspace_name = '%s' AND table_name = '%s'", d.schema(), table)
		case db.SQLITE:
			listColumnsQuery = fmt.Sprintf("PRAGMA table_info('%s')", table)
		default:
			return nil, fmt.Errorf("unsupported database type: %s", d.name())
		}

		columns, err := q.QueryContext(context.Background(), listColumnsQuery)
		if err != nil {
			return nil, fmt.Errorf("error: %w", err)
		}

		ret = append(ret, "  Columns:")

		switch d.name() {
		case db.SQLITE:
			for columns.Next() {
				var columnName, dataType string
				var unusedInt int
				var unusedBool bool
				var unusedVoid interface{}
				if err = columns.Scan(&unusedInt, &columnName, &dataType, &unusedBool, &unusedVoid, &unusedInt); err != nil {
					return nil, fmt.Errorf("error: %w\nquery: %s", err, listColumnsQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s: %s", columnName, dataType))
			}
		default:
			for columns.Next() {
				var columnName, dataType string
				if err = columns.Scan(&columnName, &dataType); err != nil {
					return nil, fmt.Errorf("error: %w\nquery: %s", err, listColumnsQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s: %s", columnName, dataType))
			}
		}
		err = columns.Close() //nolint:sqlclosecheck
		if err != nil {
			return nil, fmt.Errorf("close error: %s\nquery: %s", err, listColumnsQuery)
		}

		// Query to list indexes

		var listIndexesQuery string
		switch d.name() {
		case db.POSTGRES:
			listIndexesQuery = fmt.Sprintf("SELECT indexname, indexdef FROM pg_indexes WHERE tablename = '%s'", table)
		case db.MYSQL:
			listIndexesQuery = fmt.Sprintf("SELECT TABLE_NAME, NON_UNIQUE, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME, COLLATION, "+
				"    CARDINALITY, SUB_PART, NULLABLE, INDEX_TYPE, COMMENT "+
				"FROM "+
				"    information_schema.STATISTICS "+
				"WHERE "+
				"    TABLE_NAME = '%s';", table)
		case db.MSSQL:
			listIndexesQuery = fmt.Sprintf("SELECT name FROM sys.indexes WHERE object_id = OBJECT_ID('%s')", table)
		case db.SQLITE:
			listIndexesQuery = fmt.Sprintf("PRAGMA index_list('%s')", table)
		case db.CLICKHOUSE:
			listIndexesQuery = fmt.Sprintf("SHOW CREATE TABLE %s", table) //nolint:perfsprint
		case db.CASSANDRA:
			listIndexesQuery = fmt.Sprintf("select index_name, kind, options from system_schema.indexes where keyspace_name = '%s' and table_name = '%s'", d.schema(), table)
		default:
			return nil, fmt.Errorf("unsupported database type: %s", d.name())
		}

		indexes, err := q.QueryContext(context.Background(), listIndexesQuery)
		if err != nil {
			return nil, fmt.Errorf("error: %w", err)
		}

		ret = append(ret, "  Indexes:")
		for indexes.Next() {
			var indexName, indexDef string
			switch d.name() {
			case db.POSTGRES:
				if err = indexes.Scan(&indexName, &indexDef); err != nil {
					return nil, fmt.Errorf("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s: %s", indexName, indexDef))
			case db.MYSQL:
				var nonUnique bool
				var seqInIndex int
				var columnName, collation, indexType, comment string
				var cardinality, subPart interface{}
				var nullable string
				if err = indexes.Scan(&table, &nonUnique, &indexName, &seqInIndex, &columnName, &collation, &cardinality,
					&subPart, &nullable, &indexType, &comment); err != nil {
					return nil, fmt.Errorf("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s", indexName)) //nolint:perfsprint
			case db.MSSQL:
				if err = indexes.Scan(&indexName); err != nil {
					return nil, fmt.Errorf("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s", indexName)) //nolint:perfsprint
			case db.SQLITE:
				var seq int
				var unique, partial bool
				var origin string
				if err = indexes.Scan(&seq, &indexName, &unique, &origin, &partial); err != nil {
					return nil, fmt.Errorf("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s", indexName)) //nolint:perfsprint
			case db.CLICKHOUSE:
				var createStatement string
				if err = indexes.Scan(&createStatement); err != nil {
					return nil, fmt.Errorf("error: %s\nquery: %s", err, listIndexesQuery)
				}

				// Regular expression to find ORDER BY clause
				re := regexp.MustCompile(`ORDER BY (.*?)\n`)
				matches := re.FindStringSubmatch(createStatement)
				if len(matches) < 2 {
					return nil, fmt.Errorf("The 'ORDER BY' clause not found in the output of '%s':\n%s", listIndexesQuery, createStatement)
				}

				// Extracting columns listed in ORDER BY
				pkName := matches[1]

				ret = append(ret, fmt.Sprintf("   - %s (primary key)", pkName))
			case db.CASSANDRA:
				var idxName string
				var kind string
				var options map[string]string
				if err = indexes.Scan(&idxName, &kind, &options); err != nil {
					return nil, fmt.Errorf("error: %s\nquery: %s", err, listIndexesQuery)
				}
				ret = append(ret, fmt.Sprintf("   - %s:%s - %v", idxName, kind, options))
			default:
				return nil, fmt.Errorf("unsupported database type: %s", d.name())
			}
		}
		err = indexes.Close() //nolint:sqlclosecheck
		if err != nil {
			return nil, fmt.Errorf("close error: %s\nquery: %s", err, listIndexesQuery)
		}

		ret = append(ret, "")
	}

	return ret, nil
}

// GetTableSizeMB returns the size of a table in MB
func getTableSizeMB(q querier, d dialect, tableName string) (int64, error) {
	var query string
	var args = []interface{}{tableName}

	switch d.name() {
	case db.POSTGRES:
		query = `SELECT pg_total_relation_size('%s') / (1024 * 1024);`
	case db.MYSQL:
		query = `SELECT Data_length / (1024 * 1024) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s';`
	default:
		return -1, nil
	}

	var check = fmt.Sprintf(query, args...)
	var sizeMB int64
	if err := q.QueryRowContext(context.Background(), check).Scan(&sizeMB); err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	return sizeMB, nil
}

// GetIndexesSizeMB returns the size of indexes of a table in MB
func getIndexesSizeMB(q querier, d dialect, tableName string) (int64, error) {
	var query string
	var args = []interface{}{tableName}

	switch d.name() {
	case db.POSTGRES:
		query = "SELECT pg_indexes_size('%s') / (1024 * 1024)"
	case db.MYSQL:
		query = "SELECT Index_length / (1024 * 1024) FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '%s'"
	default:
		return -1, nil
	}

	var check = fmt.Sprintf(query, args...)
	var sizeMB int64
	if err := q.QueryRowContext(context.Background(), check).Scan(&sizeMB); err != nil && err != sql.ErrNoRows {
		return 0, err
	}

	return sizeMB, nil
}

// GetTablesVolumeInfo returns the volume info for a given set of tables
func getTablesVolumeInfo(q querier, d dialect, tableNames []string) ([]string, error) {
	var ret []string

	ret = append(ret, fmt.Sprintf("%-55s %15s %17s %17s", "TABLE NAME", "ROWS", "DATA SIZE (MB)", "IDX SIZE (MB)"))
	ret = append(ret, fmt.Sprintf("%-55s %15s %17s %17s", strings.Repeat("-", 55), strings.Repeat("-", 15), strings.Repeat("-", 17), strings.Repeat("-", 17)))

	for _, tableName := range tableNames {
		str := ""
		str += fmt.Sprintf("%-55s ", tableName)
		if exists, err := tableExists(q, d, tableName); err != nil {
			return nil, fmt.Errorf("error checking table existence: %w", err)
		} else if exists {
			var rowNum uint64
			if err = q.QueryRowContext(context.Background(), fmt.Sprintf("SELECT COUNT(*) FROM %s", d.table(tableName))).Scan(&rowNum); err != nil && err != sql.ErrNoRows {
				return nil, err
			}

			var tableSizeMB, idxSizeMB int64

			if tableSizeMB, err = getTableSizeMB(q, d, tableName); err != nil {
				return nil, fmt.Errorf("error getting table size: %w", err)
			}
			if idxSizeMB, err = getIndexesSizeMB(q, d, tableName); err != nil {
				return nil, fmt.Errorf("error getting indexes size: %w", err)
			}

			str += fmt.Sprintf("%15d %17s %17s", rowNum,
				db.TernaryStr(tableSizeMB >= 0, strconv.FormatInt(tableSizeMB, 10), "?"),
				db.TernaryStr(idxSizeMB >= 0, strconv.FormatInt(idxSizeMB, 10), "?"))
		} else {
			str += fmt.Sprintf("%15s %17s %17s", "-", "-", "-")
		}
		ret = append(ret, str)
	}

	return ret, nil
}
