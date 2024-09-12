package sql

import (
	"database/sql"
	"fmt"
	"reflect"
	"strings"

	"github.com/acronis/perfkit/db"
)

// InsertInto inserts data into a table
func (g *sqlGateway) InsertInto(tableName string, data interface{}, columnNames []string) error {
	var valuesList []reflect.Value
	v := reflect.ValueOf(data)
	var fields reflect.Type
	// var
	if v.Kind() == reflect.Slice {
		s := reflect.ValueOf(data)
		for i := 0; i < s.Len(); i++ {
			valuesList = append(valuesList, s.Index(i))
			if i == 0 {
				fields = s.Index(i).Type()
			}
		}
	} else {
		valuesList = append(valuesList, reflect.ValueOf(data))
		fields = reflect.TypeOf(data)
	}

	if len(valuesList) == 0 {
		return nil
	}

	numFields := fields.NumField()

	column2val := make(map[string]interface{})

	var columnValues []interface{}
	var valuesPlaceholders []string
	for n, values := range valuesList {
		for i := 0; i < numFields; i++ {
			columnName := fields.Field(i).Tag.Get("db")
			if columnName == "" {
				continue
			}
			column2val[columnName] = values.Field(i).Interface()
		}

		for _, col := range columnNames {
			if _, exists := column2val[col]; !exists {
				return fmt.Errorf("can't find data for column '%s' in object '%v'", col, data)
			}
			columnValues = append(columnValues, column2val[col])
		}
		if g.dialect.name() == db.CASSANDRA {
			placeholder := db.GenDBParameterPlaceholdersCassandra(n*len(columnNames), len(columnNames))
			valuesPlaceholders = append(valuesPlaceholders, fmt.Sprintf("(%s)", placeholder))
		} else {
			placeholder := db.GenDBParameterPlaceholders(n*len(columnNames), len(columnNames))
			valuesPlaceholders = append(valuesPlaceholders, fmt.Sprintf("(%s)", placeholder))
		}
	}

	query := fmt.Sprintf("INSERT INTO %s(%s) VALUES %s",
		tableName,
		strings.Join(columnNames, ", "),
		strings.Join(valuesPlaceholders, ", "))

	var result sql.Result
	var err error

	query = g.updatePlaceholders(query)
	startTime := g.StatementEnter(query, columnValues...)

	/*
		if a.DbOpts.DryRun {
			a.logger.Log("skipping the '" + query + "' request because of 'dry run' mode")
			return nil
		}

	*/

	result, err = g.rw.execContext(g.ctx, query, columnValues...)

	g.StatementExit("Exec()", startTime, err, true, result, query, columnValues, nil, nil)

	if err != nil {
		return fmt.Errorf("DB exec failed: %w", err)
	}

	return nil
}
