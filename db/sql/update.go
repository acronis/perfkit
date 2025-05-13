package sql

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/db"
)

// Update implements the databaseUpdater interface for SQL databases.
func (g *sqlGateway) Update(tableName string, c *db.UpdateCtrl) (int64, error) {
	// Get query builder for the table
	var queryBuilder, ok = tableQueryBuilders[tableName]
	if !ok {
		return 0, fmt.Errorf("table %s is not supported", tableName)
	}

	// Cast to selectBuilder to access sqlConditions
	builder, ok := queryBuilder.(selectBuilder)
	if !ok {
		return 0, fmt.Errorf("invalid query builder type for table %s", tableName)
	}

	// Build SET clause
	var setClause string
	var setArgs []interface{}
	var err error

	if len(c.Set) == 0 {
		return 0, fmt.Errorf("no values to update")
	}

	// Process SET values
	var setParts []string
	for col, values := range c.Set {
		if len(values) != 1 {
			return 0, fmt.Errorf("multiple values not supported for SET clause: %s", col)
		}
		value := values[0]

		// Handle special functions like now()
		if strings.HasSuffix(value, "()") {
			setParts = append(setParts, fmt.Sprintf("%s = %s", col, value))
		} else {
			setParts = append(setParts, fmt.Sprintf("%s = %%v", col))
			setArgs = append(setArgs, value)
		}
	}
	setClause = strings.Join(setParts, ", ")

	// Build WHERE clause using the same condition builder as Select
	var whereClause string
	var whereArgs []interface{}
	var empty bool

	if whereClause, whereArgs, empty, err = builder.sqlConditions(g.dialect, c.OptimizeConditions, c.Where); err != nil {
		return 0, err
	}

	if empty {
		return 0, nil
	}

	// Combine all parts into final query
	var query string
	if g.dialect.name() == db.CLICKHOUSE {
		// ClickHouse uses ALTER TABLE UPDATE syntax without SET keyword
		query = fmt.Sprintf("ALTER TABLE %s UPDATE %s %s", g.dialect.table(tableName), setClause, whereClause)
	} else {
		// Standard SQL UPDATE syntax for other databases
		query = fmt.Sprintf("UPDATE %s SET %s %s", g.dialect.table(tableName), setClause, whereClause)
	}

	// Add explain prefix if needed
	if g.explain {
		if query, err = addExplainPrefix(g.dialect.name(), query); err != nil {
			return 0, err
		}
	}

	// Execute query with encoded arguments
	query = sqlf(g.dialect, query, append(setArgs, whereArgs...)...)
	result, err := g.rw.execContext(g.ctx, query)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}
