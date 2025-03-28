package sql

import (
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/db/optimize"
)

// tableQueryBuilders maps table names to their corresponding query builders
// Used to cache query builders for better performance
var tableQueryBuilders = make(map[string]selectBuilder)

// createSelectQueryBuilder creates a new query builder for a table
// Parameters:
// - tableName: name of the table to create builder for
// - tableRows: schema definition of the table columns
// Returns error if builder creation fails
func createSelectQueryBuilder(tableName string, tableRows []db.TableRow) error {
	// Skip if builder already exists for this table
	if _, ok := tableQueryBuilders[tableName]; ok {
		return nil
	}

	// Create new builder with table name and empty queryable map
	var queryBuilder = selectBuilder{
		tableName: tableName,
		queryable: make(map[string]filterFunction),
	}

	// Add filter functions for each column based on its data type
	// These filter functions implement the query building logic for WHERE clauses
	for _, row := range tableRows {
		switch row.Type {
		case db.DataTypeInt, db.DataTypeBigInt, db.DataTypeBigIntAutoIncPK,
			db.DataTypeBigIntAutoInc, db.DataTypeSmallInt, db.DataTypeTinyInt:
			queryBuilder.queryable[row.Name] = idCond() // Numeric ID conditions
		case db.DataTypeUUID, db.DataTypeVarCharUUID:
			queryBuilder.queryable[row.Name] = uuidCond() // UUID conditions
		case db.DataTypeVarChar, db.DataTypeVarChar32, db.DataTypeVarChar36,
			db.DataTypeVarChar64, db.DataTypeVarChar128, db.DataTypeVarChar256,
			db.DataTypeText, db.DataTypeLongText:
			queryBuilder.queryable[row.Name] = stringCond(256, true) // String conditions with LIKE support
		case db.DataTypeDateTime, db.DataTypeDateTime6,
			db.DataTypeTimestamp, db.DataTypeTimestamp6,
			db.DataTypeCurrentTimeStamp6:
			queryBuilder.queryable[row.Name] = timeCond() // Timestamp conditions
		}
	}

	// Cache the builder for future use
	tableQueryBuilders[tableName] = queryBuilder

	return nil
}

// minTime and maxTime define the supported timestamp range
var minTime = time.Unix(-2208988800, 0) // Jan 1, 1900
var maxTime = time.Unix(1<<63-62135596801, 999999999)

// filterFunction is a function type that generates SQL conditions
// Parameters:
// - d: SQL dialect being used
// - optimizeConditions: whether to use optimized condition building
// - field: database field name
// - values: condition values to filter on
// Returns generated SQL fragments and arguments
type filterFunction func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error)

// selectBuilder handles SQL SELECT query construction
type selectBuilder struct {
	tableName string                    // Name of the table being queried
	queryable map[string]filterFunction // Maps column names to their filter functions
}

// sqlOrder generates the ORDER BY clause
// Parameters:
// - fields: columns to select
// - values: order specifications (e.g. "asc(field)", "desc(field)")
// Returns the ORDER BY clause or error
func (b selectBuilder) sqlOrder(d dialect, fields []string, values []string) (string, error) {
	var result = ""
	if len(fields) == 0 { // select count
		return result, nil
	}

	for _, v := range values {
		fnc, args, err := db.ParseFuncMultipleArgs(v, ";")
		if err != nil {
			return "", err
		}

		if len(args) == 0 {
			return "", fmt.Errorf("empty order field")
		}

		var dir string

		switch fnc {
		case "asc":
			dir = "ASC"
		case "desc":
			dir = "DESC"
		case "nearest":
			dir = "NEAREST"
		case "":
			return "", fmt.Errorf("empty order function")
		default:
			return "", fmt.Errorf("bad order function '%v'", fnc)
		}

		var orderStatement string
		if dir == "ASC" || dir == "DESC" {
			if len(args) != 1 {
				return "", fmt.Errorf("number of args %d doesn't match number of conditions 1", len(args))
			}

			orderStatement = fmt.Sprintf("%v.%v %v", b.tableName, args[0], dir)
		} else if dir == "NEAREST" {
			if len(args) != 3 {
				return "", fmt.Errorf("number of args %d doesn't match number of conditions for nearest function, should be 3", len(args))
			}

			var field = args[0]
			var operator = args[1]
			var vector = args[2]

			// Use dialect-specific vector ordering
			orderStatement = d.encodeOrderByVector(field, operator, vector)
		}

		if result == "" {
			result = fmt.Sprintf("ORDER BY %s", orderStatement)
		} else {
			result += fmt.Sprintf(", %s", orderStatement)
		}
	}

	return result, nil
}

// sqlSelectionAlias generates the SELECT clause with optional table alias
// Parameters:
// - fields: columns to select
// - alias: optional table alias
// Returns the SELECT clause or error
func (b selectBuilder) sqlSelectionAlias(fields []string, alias string) (string, error) {
	if len(fields) == 1 && fields[0] == "COUNT(0)" { // select count
		return "SELECT COUNT(0)", nil
	}

	var columns []string
	var addFields = func(fields []string) {
		for _, c := range fields {
			if _, _, err := db.ParseFunc(c); err == nil {
				columns = append(columns, c)
			} else {
				if alias != "" {
					c = alias + "." + c
				}
				columns = append(columns, c)
			}
		}
	}

	for _, field := range fields {
		if field == "" {
			return "", fmt.Errorf("empty select field")
		}

		addFields([]string{field})
	}

	return "SELECT " + strings.Join(columns, ", "), nil
}

// sqlConditions generates the WHERE clause
// Parameters:
// - d: SQL dialect
// - optimizeConditions: whether to use optimized condition building
// - fields: map of column names to filter values
// Returns WHERE clause, arguments, and error
func (b selectBuilder) sqlConditions(d dialect, optimizeConditions bool, fields map[string][]string) (string, []interface{}, bool, error) {
	var fmtString = ""
	var fmtArgs []interface{}

	var addFmt = func(fmts string, arg interface{}) {
		if fmtString == "" {
			fmtString = "WHERE " + fmts
		} else {
			fmtString += " AND " + fmts
		}

		if arg != nil {
			fmtArgs = append(fmtArgs, arg)
		}
	}

	for _, c := range db.SortFields(fields) {
		if c.Col == "" {
			return "", nil, false, fmt.Errorf("empty condition field")
		}

		condgen, ok := b.queryable[c.Col]
		if !ok {
			return "", nil, false, fmt.Errorf("bad condition field '%v'", c.Col)
		}

		if len(c.Vals) == 1 {
			// generic special cases
			if c.Vals[0] == db.SpecialConditionIsNull {
				addFmt(fmt.Sprintf("%v.%v IS %%v", b.tableName, c.Col), sql.NullString{})
				continue
			}
			if c.Vals[0] == db.SpecialConditionIsNotNull {
				addFmt(fmt.Sprintf("%v.%v IS NOT %%v", b.tableName, c.Col), sql.NullString{})
				continue
			}
		}

		fmts, args, err := condgen(d, optimizeConditions, fmt.Sprintf("%v.%v", b.tableName, c.Col), c.Vals)
		if err != nil {
			return "", nil, false, err
		}

		if fmts == nil {
			continue
		}

		if len(fmts) != len(args) {
			return "", nil, false, fmt.Errorf("number of args %d doesn't match number of conditions %d", len(args), len(fmts))
		}

		for i := range fmts {
			addFmt(fmts[i], args[i])
		}
	}

	if len(fields) != 0 && len(fmtArgs) == 0 {
		return "", nil, true, nil
	}

	return fmtString, fmtArgs, false, nil
}

// sqlf formats SQL query with dialect-specific value encoding
// Implements the query string interpolation specified in db.Config.QueryStringInterpolation
// Parameters:
// - d: SQL dialect for value encoding
// - fmts: format string
// - args: values to format
// Returns formatted SQL string
func sqlf(d dialect, fmts string, args ...interface{}) string {
	if len(args) == 0 && !strings.Contains(fmts, "%") {
		return fmts // Avoid fmt.Sprintf overhead and memory allocation.
	}
	for i, v := range args {
		switch val := v.(type) {
		case sql.NullString:
			if !val.Valid {
				args[i] = "NULL"
			} else {
				args[i] = d.encodeString(val.String)
			}

		case sql.NullInt64:
			if !val.Valid {
				args[i] = "NULL"
			} else {
				args[i] = val.Int64
			}

		case sql.NullInt32:
			if !val.Valid {
				args[i] = "NULL"
			} else {
				args[i] = val.Int32
			}

		case string:
			args[i] = d.encodeString(val)

		case uuid.UUID:
			args[i] = d.encodeUUID(val)

		case []string:
			var sb strings.Builder
			for j := range val {
				if j != 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(d.encodeString(val[j]))
			}
			args[i] = sb.String()

		case []byte:
			if len(val) == 0 {
				args[i] = "NULL"
			} else {
				args[i] = d.encodeBytes(val)
			}
		case []int64:
			var sb strings.Builder
			for _, i := range val {
				if sb.Len() != 0 {
					sb.WriteByte(',')
				}
				sb.WriteString(strconv.FormatInt(i, 10))
			}
			args[i] = sb.String()

		case []float32:
			args[i] = d.encodeVector(val)

		case bool:
			args[i] = d.encodeBool(val)

		case time.Time:
			args[i] = d.encodeTime(val)
		}
	}

	return fmt.Sprintf(fmts, args...)
}

// sql generates the complete SQL query
// Parameters:
// - d: SQL dialect
// - c: selection control parameters
// Returns complete SQL query string and error
func (b selectBuilder) sql(d dialect, c *db.SelectCtrl) (string, bool, error) {
	var selectWhat, where, order, limit string
	var err error
	var args []interface{}
	var empty bool

	if selectWhat, err = b.sqlSelectionAlias(c.Fields, b.tableName); err != nil {
		return "", false, err
	}

	if where, args, empty, err = b.sqlConditions(d, c.OptimizeConditions, c.Where); err != nil {
		return "", false, err
	}

	if empty {
		return "", true, nil
	}

	if order, err = b.sqlOrder(d, c.Fields, c.Order); err != nil {
		return "", false, err
	}

	if c.Page.Limit > 0 {
		if d.name() != db.MSSQL {
			limit = fmt.Sprintf("LIMIT %v OFFSET %v", c.Page.Limit, c.Page.Offset)
		} else {
			if order == "" {
				order = "ORDER BY id DESC"
			} else {
				limit = fmt.Sprintf("OFFSET %v ROWS FETCH NEXT %v ROWS ONLY", c.Page.Offset, c.Page.Limit)
			}
		}
	}

	fromWhere := fmt.Sprintf("FROM %s", d.table(b.tableName))
	qry := b.build(selectWhat, fromWhere, where, order, limit)

	return sqlf(d, qry, args...), false, nil
}

// build combines SQL query components into final query
// Parameters:
// - do: SELECT clause
// - from: FROM clause
// - where: WHERE clause
// - orderBy: ORDER BY clause
// - limit: LIMIT clause
// Returns complete SQL query
func (b selectBuilder) build(do, from, where, orderBy, limit string) string {
	return fmt.Sprintf("%s %s %s %s %s", do, from, where, orderBy, limit)
}

// convFnc converts function names to SQL operators
// Parameters:
// - fnc: function name (e.g. "lt", "gt", "le", "ge")
// - field: field name
// Returns SQL condition fragment
func convFnc(fnc string, field string) (string, error) {
	cond := ""
	switch fnc {
	case "":
		cond = field + " = %v"
	case "lt":
		cond = field + " < %v"
	case "le":
		cond = field + " <= %v"
	case "gt":
		cond = field + " > %v"
	case "ge":
		cond = field + " >= %v"
	default:
		return cond, fmt.Errorf("unsupported function")
	}
	return cond, nil
}

// optimizedIdCond implements optimized ID condition building using the optimize package
// Parameters:
// - d: SQL dialect for value encoding
// - field: database field name
// - values: array of ID conditions (can include functions like "lt(5)", "gt(10)")
// Returns SQL fragments and arguments for the WHERE clause
func optimizedIdCond(d dialect, field string, values []string) ([]string, []interface{}, error) {
	// Uses optimize.IDCond to process ID ranges and equality conditions efficiently
	var minVal, maxVal, equalityRange, empty, err = optimize.IDCond(field, values)
	if err != nil {
		return nil, nil, err
	}

	if empty {
		return []string{}, []interface{}{}, err
	}

	var minInt = int64(math.MinInt64)
	var maxInt = int64(math.MaxInt64)

	var vals []interface{}
	var conds []string

	// Handle equality conditions (IN clause optimization)
	if len(equalityRange) != 0 {
		if len(equalityRange) == 1 {
			conds = append(conds, field+" = %v")
			vals = append(vals, equalityRange[0])
		} else if len(equalityRange) > 1 {
			conds = append(conds, field+" IN (%v)")
			vals = append(vals, equalityRange)
		}
	} else {
		// Handle range conditions
		if minVal != minInt {
			conds = append(conds, field+" > %v")
			vals = append(vals, minVal)
		}

		if maxVal != maxInt {
			conds = append(conds, field+" < %v")
			vals = append(vals, maxVal)
		}
	}

	return conds, vals, nil
}

// str2id converts a string to int64
// Used by ID condition builders to parse numeric values
func str2id(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

// nonOptimizedIdCond implements standard ID condition building without optimizations
// Parameters:
// - d: SQL dialect for value encoding
// - field: database field name
// - values: array of ID conditions
// Returns SQL fragments and arguments for the WHERE clause
func nonOptimizedIdCond(d dialect, field string, values []string) ([]string, []interface{}, error) {
	// Helper function to process individual values and extract function and ID
	var procVal = func(v string) (string, int64, error) {
		var id int64
		fnc, val, parseErr := db.ParseFunc(v)
		if parseErr != nil {
			return "", 0, fmt.Errorf("%v on field '%v'", parseErr, field)
		}

		id, parseErr = str2id(val)
		if parseErr != nil {
			return "", 0, fmt.Errorf("%v on field '%v'", parseErr, field)
		}

		return fnc, id, nil
	}

	var vals []interface{}
	var equality []int64
	var conds []string

	// Process each value and build appropriate conditions
	for _, v := range values {
		fnc, id, err := procVal(v)
		if err != nil {
			return nil, nil, err
		}

		if fnc == "" {
			equality = append(equality, id)
		} else {
			var cond string
			if cond, err = convFnc(fnc, field); err != nil {
				return nil, nil, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, field)
			}
			conds = append(conds, cond)
			vals = append(vals, id)
		}
	}

	// Handle equality conditions
	if len(equality) == 1 {
		conds = append(conds, field+" = %v")
		vals = append(vals, equality[0])
	} else if len(equality) > 1 {
		conds = append(conds, field+" IN (%v)")
		vals = append(vals, equality)
	}

	return conds, vals, nil
}

// idCond returns a filterFunction for handling ID fields
// Supports both optimized and non-optimized condition building based on configuration
func idCond() filterFunction {
	var fn = func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error) {
		if optimizeConditions {
			return optimizedIdCond(d, field, values)
		} else {
			return nonOptimizedIdCond(d, field, values)
		}
	}

	return fn
}

// uuidCond returns a filterFunction for handling UUID fields
// Implements UUID-specific condition building with proper type conversion
func uuidCond() filterFunction {
	var fn = func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error) {
		// Helper function to process UUID values and extract function
		var procVal = func(v string) (string, string, error) {
			var id uuid.UUID
			fnc, val, err := db.ParseFunc(v)
			if err != nil {
				return "", "", fmt.Errorf("%v on uuid field", err)
			}

			id, err = uuid.ParseBytes([]byte(val))
			if err != nil {
				return "", "", fmt.Errorf("bad uuid format: %v", err)
			}

			return fnc, id.String(), nil
		}

		var vals []interface{}
		var equality []string
		var conds []string

		// Process each UUID value
		for _, v := range values {
			fnc, id, err := procVal(v)
			if err != nil {
				return nil, nil, fmt.Errorf("%v on field '%v'", err, field)
			}

			if fnc == "" {
				equality = append(equality, id)
			} else {
				var cond string
				if cond, err = convFnc(fnc, field); err != nil {
					return nil, nil, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, field)
				}
				conds = append(conds, cond)
				vals = append(vals, id)
			}
		}

		// Handle equality conditions for UUIDs
		if len(equality) == 1 {
			conds = append(conds, field+" = %v")
			vals = append(vals, equality[0])
		} else if len(equality) > 1 {
			conds = append(conds, field+" IN (%v)")
			vals = append(vals, equality)
		}

		return conds, vals, nil
	}

	return fn
}

// stringCond returns a filterFunction for handling string fields
// Parameters:
// - maxValueLen: maximum allowed length for string values
// - allowLikes: whether LIKE operations are permitted
// Returns a filterFunction for string operations
func stringCond(maxValueLen int, allowLikes bool) filterFunction {
	var fn = func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error) {
		// Helper function to process and validate string values
		procValue := func(field, value string) (string, string, error) {
			fnc, val, err := db.ParseFunc(value)
			if err != nil {
				return "", "", fmt.Errorf("%v on field '%v'", err, field)
			}

			if val == "" {
				return "", "", fmt.Errorf("field '%v' value is empty", field)
			}
			if len(val) > maxValueLen {
				return "", "", fmt.Errorf("field '%v' value is too long, max length is %v", field, maxValueLen)
			}

			return fnc, val, nil
		}

		var conds []string
		var vals []interface{}

		// Specification of translation API filters to SQL conditions:
		// 1. field  = value1 && field  = value2  ->  field IN (value1, value2)
		// 2. field != value1 && field != value2  ->  field NOT IN (value1, value2)
		// 3. field  = value1 && field != value2  ->  field IN (value1)
		// 4. field  = value1 && field != value1  ->  error
		var positiveFilter = false
		var valuesPositive []string
		var valuesNegative []string

		// Process each value and build appropriate conditions
		for _, v := range values {
			fnc, val, err := procValue(field, v)
			if err != nil {
				return nil, nil, err
			}

			// Handle different string comparison functions
			switch fnc {
			case "": // Exact match
				positiveFilter = true
				valuesPositive = append(valuesPositive, val)
			case "ne": // Not equal
				valuesNegative = append(valuesNegative, val)
			case "hlike": // LIKE with suffix wildcard
				if !allowLikes {
					return nil, nil, fmt.Errorf("like functions are unsupported on field '%v'", field)
				}
				conds = append(conds, field+" LIKE %v")
				vals = append(vals, val+"%%")
			case "tlike": // LIKE with prefix wildcard
				if !allowLikes {
					return nil, nil, fmt.Errorf("like functions are unsupported on field '%v'", field)
				}
				conds = append(conds, field+" LIKE %v")
				vals = append(vals, "%%"+val)
			case "like": // LIKE with both wildcards
				if !allowLikes {
					return nil, nil, fmt.Errorf("like functions are unsupported on field '%v'", field)
				}
				conds = append(conds, field+" LIKE %v")
				vals = append(vals, "%%"+val+"%%")
			case "lt", "le", "gt", "ge": // Comparison operators
				var cond string
				if cond, err = convFnc(fnc, field); err != nil {
					return nil, nil, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, field)
				}
				conds = append(conds, cond)
				vals = append(vals, val)
			default:
				return nil, nil, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, field)
			}
		}

		// Check for conflicting conditions
		var valuesNegativeSet = map[string]struct{}{}
		for _, v := range valuesNegative {
			valuesNegativeSet[v] = struct{}{}
		}

		for _, v := range valuesPositive {
			if _, ok := valuesNegativeSet[v]; ok {
				return nil, nil, fmt.Errorf("positive condition on value cannot be set alongside with ne function on it, field '%v', value '%v'", field, v)
			}
		}

		// Build final conditions for equality/inequality
		if len(valuesPositive) > 0 || len(valuesNegative) > 0 {
			if positiveFilter {
				if len(valuesPositive) == 1 {
					conds = append(conds, field+" = %v")
					vals = append(vals, valuesPositive[0])
				} else {
					conds = append(conds, field+" IN (%v)")
					vals = append(vals, valuesPositive)
				}
			} else {
				if len(valuesNegative) == 1 {
					conds = append(conds, field+" <> %v")
					vals = append(vals, valuesNegative[0])
				} else {
					conds = append(conds, field+" NOT IN (%v)")
					vals = append(vals, valuesNegative)
				}
			}
		}

		return conds, vals, nil
	}

	return fn
}

func optimizedEnumStringCond(d dialect, conv func(string) (int64, error), enumSize int, enumValues []int64, field string, values []string) ([]string, []interface{}, error) {
	// Use optimize package to process enum conditions efficiently
	var equalityRange, empty, err = optimize.EnumStringCond(field, values, conv, enumSize, enumValues)
	if err != nil {
		return nil, nil, err
	}

	if empty {
		return []string{}, []interface{}{}, nil
	}

	var conds []string
	var vals []interface{}

	// Build conditions based on number of matches
	switch len(equalityRange) {
	case 0:
		// No matches, return empty conditions
	case 1:
		// Single match, use equality
		conds = append(conds, field+" = %v")
		vals = append(vals, equalityRange[0])
	default:
		// Multiple matches, use IN clause
		conds = append(conds, field+" IN (%v)")
		vals = append(vals, equalityRange)
	}

	return conds, vals, nil
}

func nonOptimizedEnumStringCond(d dialect, conv func(string) (int64, error), enumSize int, enumValues []int64, field string, values []string) ([]string, []interface{}, error) {
	// Helper function to process enum values
	var procVal = func(v string) (string, int64, error) {
		var id int64
		fnc, val, parseErr := db.ParseFunc(v)
		if parseErr != nil {
			return "", 0, fmt.Errorf("%v on field '%v'", parseErr, field)
		}

		id, parseErr = conv(val)
		if parseErr != nil {
			return "", 0, fmt.Errorf("%v on field '%v'", parseErr, field)
		}

		return fnc, id, nil
	}

	// Remove duplicates from input values
	dvalues := db.DedupStrings(values)
	if len(dvalues) == 0 {
		return nil, nil, nil
	}

	var vals []interface{}
	var equality []int64
	var conds []string

	// Process each enum value
	for _, v := range dvalues {
		fnc, id, err := procVal(v)
		if err != nil {
			return nil, nil, err
		}
		if fnc == "" {
			equality = append(equality, id)
		} else {
			var cond string
			if cond, err = convFnc(fnc, field); err != nil {
				return nil, nil, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, field)
			}
			conds = append(conds, cond)
			vals = append(vals, id)
		}
	}

	// Build final conditions
	if len(equality) == 1 {
		conds = append(conds, field+" = %v")
		vals = append(vals, equality[0])
	} else if len(equality) > 1 {
		conds = append(conds, field+" IN (%v)")
		vals = append(vals, equality)
	}

	return conds, vals, nil
}

func enumStringCond(conv func(string) (int64, error), enumSize int, enumValues []int64) filterFunction {
	var fn = func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error) {
		if optimizeConditions {
			return optimizedEnumStringCond(d, conv, enumSize, enumValues, field, values)
		} else {
			return nonOptimizedEnumStringCond(d, conv, enumSize, enumValues, field, values)
		}
	}

	return fn
}

func optimizedTimeCond(field string, values []string) ([]string, []interface{}, error) {
	var minVal, maxVal, equalityRange, empty, err = optimize.TimeCond(field, values)
	if err != nil {
		return nil, nil, err
	}

	if empty {
		return []string{}, []interface{}{}, err
	}

	var conds []string
	var vals []interface{}

	// Handle equality conditions (IN clause optimization)
	if len(equalityRange) != 0 {
		var equalInRange []int64
		for _, val := range equalityRange {
			equalInRange = append(equalInRange, val.UnixNano())
		}

		if len(equalInRange) == 1 {
			conds = append(conds, field+" = %v")
			vals = append(vals, equalInRange[0])
		} else if len(equalInRange) > 1 {
			conds = append(conds, field+" IN (%v)")
			vals = append(vals, equalInRange)
		}
	} else {
		// Handle range conditions
		if minVal != minTime {
			conds = append(conds, field+" > %v")
			vals = append(vals, minVal.UnixNano())
		}
		if maxVal != maxTime {
			conds = append(conds, field+" < %v")
			vals = append(vals, maxVal.UnixNano())
		}
	}

	return conds, vals, nil
}

func nonOptimizedTimeCond(field string, values []string) ([]string, []interface{}, error) {
	var vals []interface{}
	var conds []string

	// Process each timestamp value and build conditions
	for _, v := range values {
		// Parse function and timestamp value
		fnc, val, err := db.ParseFunc(v)
		if err != nil {
			return nil, nil, fmt.Errorf("%v on field '%v'", err, field)
		}

		// Parse timestamp in UTC
		var t time.Time
		t, err = db.ParseTimeUTC(val)
		if err != nil {
			return nil, nil, fmt.Errorf("%v on field '%v'", err, field)
		}

		// Convert function to SQL operator
		var cond string
		if cond, err = convFnc(fnc, field); err != nil {
			return nil, nil, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, field)
		}

		conds = append(conds, cond)
		vals = append(vals, t.UnixNano())
	}

	return conds, vals, nil
}

func timeCond() filterFunction {
	var fn = func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error) {
		if optimizeConditions {
			return optimizedTimeCond(field, values)
		} else {
			return nonOptimizedTimeCond(field, values)
		}
	}

	return fn
}

func (g *sqlGateway) Select(tableName string, sc *db.SelectCtrl) (db.Rows, error) {
	var queryBuilder, ok = tableQueryBuilders[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s is not supported", tableName)
	}

	var query, empty, err = queryBuilder.sql(g.dialect, sc)
	if err != nil {
		return nil, err
	}

	if empty {
		return &db.EmptyRows{}, nil
	}

	if g.explain {
		if query, err = addExplainPrefix(g.dialect.name(), query); err != nil {
			return &db.EmptyRows{}, err
		}
	}

	var rows *sql.Rows
	rows, err = g.rw.queryContext(g.ctx, query)

	if g.explain && g.explainLogger != nil {
		if err = logExplainResults(g.explainLogger, g.dialect.name(), rows, query); err != nil {
			return &db.EmptyRows{}, err
		}
	}

	return &wrappedRows{rows: rows, readRowsLogger: g.readRowsLogger}, nil
}
