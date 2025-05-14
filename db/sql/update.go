package sql

import (
	"fmt"
	"strings"

	"github.com/acronis/perfkit/db"
)

// updateQueryBuilders maps table names to their corresponding query builders
// Used to cache query builders for better performance
var updateQueryBuilders = make(map[string]updateQueryBuilder)

// updateQueryBuilder is an interface for generating SQL UPDATE queries
// It defines the methods required for building SQL UPDATE queries
type updateQueryBuilder interface {
	sql(d dialect, c *db.UpdateCtrl) (string, []interface{}, error) // Generates SQL UPDATE query
}

// newUpdateQueryBuilder is a factory method for creating a new updateQueryBuilder
func (queryBuildersFactory *defaultQueryBuildersFactory) newUpdateQueryBuilder(tableName string, queryable map[string]filterFunction) updateQueryBuilder {
	return &updateBuilder{
		tableName: tableName,
		queryable: queryable,
	}
}

// updateBuilder is a struct that implements the updateQueryBuilder interface
type updateBuilder struct {
	tableName string                    // Name of the table
	queryable map[string]filterFunction // queryable is a map of queryable fields and their corresponding filter functions
}

func (ub *updateBuilder) sql(d dialect, c *db.UpdateCtrl) (string, []interface{}, error) {
	if len(c.Set) == 0 {
		return "", nil, fmt.Errorf("no values to update")
	}

	// Process SET values
	var setParts []string
	var setArgs []interface{}
	for col, value := range c.Set {
		// Handle special functions like now()
		if strValue, ok := value.(string); ok && strings.HasSuffix(strValue, "()") {
			setParts = append(setParts, fmt.Sprintf("%s = %s", col, strValue))
		} else {
			setParts = append(setParts, fmt.Sprintf("%s = %%v", col))
			setArgs = append(setArgs, value)
		}
	}
	setClause := strings.Join(setParts, ", ")

	// Build WHERE clause using the same condition builder as Select
	var whereClause string
	var whereArgs []interface{}
	var empty bool
	var err error

	if whereClause, whereArgs, empty, err = sqlConditions(d, ub.tableName, ub.queryable, c.OptimizeConditions, c.Where); err != nil {
		return "", nil, err
	}

	if empty {
		return "", nil, nil
	}

	// Combine all parts into final query
	var query string
	if d.name() == db.CLICKHOUSE {
		// ClickHouse uses ALTER TABLE UPDATE syntax without SET keyword
		query = fmt.Sprintf("ALTER TABLE %s UPDATE %s %s", d.table(ub.tableName), setClause, whereClause)
	} else {
		// Standard SQL UPDATE syntax for other databases
		query = fmt.Sprintf("UPDATE %s SET %s %s", d.table(ub.tableName), setClause, whereClause)
	}

	// Ensure all parameters are properly set before formatting
	allArgs := append(setArgs, whereArgs...)
	if len(allArgs) == 0 {
		return query, nil, nil
	}

	return sqlf(d, query, allArgs...), nil, nil
}

// Update implements the databaseUpdater interface for SQL databases.
func (g *sqlGateway) Update(tableName string, c *db.UpdateCtrl) (int64, error) {
	var updQueryBuilder, ok = updateQueryBuilders[tableName]
	if !ok {
		return 0, fmt.Errorf("table %s is not supported", tableName)
	}

	// Generate the SQL query and values using the updateQueryBuilder interface
	var query, values, err = updQueryBuilder.sql(g.dialect, c)
	if err != nil {
		return 0, err
	}

	// Add explain prefix if needed
	if g.explain {
		if query, err = addExplainPrefix(g.dialect.name(), query); err != nil {
			return 0, err
		}
	}

	// Execute query with encoded arguments
	result, err := g.rw.execContext(g.ctx, query, values...)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}
