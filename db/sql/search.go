package sql

import (
	"database/sql"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/db/optimize"
)

var tableQueryBuilders = make(map[string]selectBuilder)

func createSelectQueryBuilder(tableName string, tableRows []db.TableRow) error {
	if _, ok := tableQueryBuilders[tableName]; ok {
		return nil
	}

	var queryBuilder = selectBuilder{
		tableName: tableName,
		queryable: make(map[string]filterFunction),
	}

	for _, row := range tableRows {
		switch row.Type {
		case db.DataTypeInt:
			queryBuilder.queryable[row.Name] = idCond()
		case db.DataTypeUUID:
			queryBuilder.queryable[row.Name] = uuidCond()
		case db.DataTypeString256, db.DataTypeLongText:
			queryBuilder.queryable[row.Name] = stringCond(256, false)
		case db.DataTypeDateTime:
			queryBuilder.queryable[row.Name] = timeCond()
		}
	}

	tableQueryBuilders[tableName] = queryBuilder

	return nil
}

// rUpdatePlaceholders is a regexp to replace placeholders
var rUpdatePlaceholders = regexp.MustCompile(`\$\d+`)

// updatePlaceholders replaces placeholders
func (g *sqlGateway) updatePlaceholders(query string) string {
	if g.dialect.name() == db.MYSQL || g.dialect.name() == db.SQLITE || g.dialect.name() == db.CASSANDRA {
		return rUpdatePlaceholders.ReplaceAllString(query, "?")
	}

	return query
}

// SearchRaw executes a query and returns the result set as a slice of maps
func (g *sqlGateway) SearchRaw(from string, what string, where string, orderBy string, limit int, explain bool, args ...interface{}) (db.Rows, error) {
	var query string

	switch g.dialect.name() {
	case db.MSSQL:
		query = fmt.Sprintf("SELECT {LIMIT} %s FROM %s {WHERE} {ORDERBY}", what, g.dialect.table(from))
	default:
		query = fmt.Sprintf("SELECT %s FROM %s {WHERE} {ORDERBY} {LIMIT}", what, g.dialect.table(from))
	}

	if where == "" {
		query = strings.Replace(query, "{WHERE}", "", -1)
	} else {
		query = strings.Replace(query, "{WHERE}", fmt.Sprintf("WHERE %s", where), -1) //nolint:perfsprint
	}

	if limit == 0 {
		query = strings.Replace(query, "{LIMIT}", "", -1)
	} else {
		switch g.dialect.name() {
		case db.MSSQL:
			query = strings.Replace(query, "{LIMIT}", fmt.Sprintf("TOP %d", limit), -1)
		default:
			query = strings.Replace(query, "{LIMIT}", fmt.Sprintf("LIMIT %d", limit), -1)
		}
	}

	if orderBy == "" {
		query = strings.Replace(query, "{ORDERBY}", "", -1)
	} else {
		query = strings.Replace(query, "{ORDERBY}", fmt.Sprintf("ORDER BY %s", orderBy), -1) //nolint:perfsprint
	}

	query = g.updatePlaceholders(query)

	var rows *sql.Rows
	var err error
	startTime := g.StatementEnter(query, args)

	if explain {
		query, err = g.addExplainPrefix(query)
	}
	if err != nil {
		return nil, err
	}

	rows, err = g.rw.queryContext(g.ctx, query, args...)

	if explain {
		return nil, g.explain(rows, query, args...)
	}

	if err != nil {
		return nil, fmt.Errorf("DB query failed: %w", err)
	}

	g.StatementExit("Query()", startTime, err, false, nil, query, args, nil, nil)

	return &sqlRows{rows: rows}, nil
}

// addExplainPrefix adds an 'explain' prefix to the query
func (g *sqlGateway) addExplainPrefix(query string) (string, error) {
	switch g.dialect.name() {
	case db.MYSQL:
		return "EXPLAIN " + query, nil
	case db.POSTGRES:
		return "EXPLAIN ANALYZE " + query, nil
	case db.SQLITE:
		return "EXPLAIN QUERY PLAN " + query, nil
	case db.CASSANDRA:
		return "TRACING ON; " + query, nil
	default:
		return "", fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", g.dialect.name())
	}
}

// explain executes an 'explain' query
func (g *sqlGateway) explain(rows *sql.Rows, query string, args ...interface{}) error {
	// Iterate over the result set
	cols, err := rows.Columns()
	if err != nil {
		return fmt.Errorf("DB query failed: %s\nError: %s", query, err)
	}

	values := make([]sql.RawBytes, len(cols))
	scanArgs := make([]interface{}, len(values))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	g.queryLogger.Log("\n%s", query)
	if args != nil {
		g.queryLogger.Log(" %v\n", args)
	} else {
		g.queryLogger.Log("\n")
	}

	for rows.Next() {
		switch g.dialect.name() {
		case db.SQLITE:
			var id, parent, notUsed int
			var detail string
			if err = rows.Scan(&id, &parent, &notUsed, &detail); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			g.queryLogger.Log("ID: %d, Parent: %d, Not Used: %d, Detail: %s\n", id, parent, notUsed, detail)
		case db.MYSQL:
			if err = rows.Scan(scanArgs...); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			// Print each column as a string.
			for i, col := range values {
				g.queryLogger.Log("  %-15s: %s\n", cols[i], string(col))
			}
			g.queryLogger.Log("\n")
		case db.POSTGRES:
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			g.queryLogger.Log("  ", explainOutput)
		case db.CASSANDRA:
			var explainOutput string
			if err = rows.Scan(&explainOutput); err != nil {
				return fmt.Errorf("DB query result scan failed: %s\nError: %s", query, err)
			}
			g.queryLogger.Log("  ", explainOutput)
		default:
			return fmt.Errorf("the 'explain' mode is not supported for given database driver: %s", g.dialect.name())
		}
	}

	return nil
}

var minTime = time.Unix(-2208988800, 0) // Jan 1, 1900
var maxTime = time.Unix(1<<63-62135596801, 999999999)

type filterFunction func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error)

type selectBuilder struct {
	tableName string
	queryable map[string]filterFunction
}

func (b selectBuilder) sqlOrder(fields []string, values []string) (string, error) {
	var result = ""
	if len(fields) == 0 { // select count
		return result, nil
	}

	for _, v := range values {
		fnc, field, err := db.ParseFunc(v)
		if err != nil {
			return "", err
		}

		if field == "" {
			return "", fmt.Errorf("empty order field")
		}

		var dir string

		switch fnc {
		case "asc":
			dir = "ASC"
		case "desc":
			dir = "DESC"
		case "":
			return "", fmt.Errorf("empty order function")
		default:
			return "", fmt.Errorf("bad order function '%v'", fnc)
		}

		if result == "" {
			result = fmt.Sprintf("ORDER BY %v.%v %v", b.tableName, field, dir)
		} else {
			result += fmt.Sprintf(", %v.%v %v", b.tableName, field, dir)
		}
	}

	return result, nil
}

func (b selectBuilder) sqlSelectionAlias(fields []string, alias string) (string, error) {
	if len(fields) == 0 { // select count
		return "SELECT COUNT(0)", nil
	}

	var columns []string
	var addFields = func(fields []string) {
		for _, c := range fields {
			if alias != "" {
				c = alias + "." + c
			}
			columns = append(columns, c)
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

		case bool:
			args[i] = d.encodeBool(val)
		}
	}

	return fmt.Sprintf(fmts, args...)
}

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

	if order, err = b.sqlOrder(c.Fields, c.Order); err != nil {
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

func (b selectBuilder) build(do, from, where, orderBy, limit string) string {
	return fmt.Sprintf("%s %s %s %s %s", do, from, where, orderBy, limit)
}

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

func optimizedIdCond(d dialect, field string, values []string) ([]string, []interface{}, error) {
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

	if len(equalityRange) != 0 {
		if len(equalityRange) == 1 {
			conds = append(conds, field+" = %v")
			vals = append(vals, equalityRange[0])
		} else if len(equalityRange) > 1 {
			conds = append(conds, field+" IN (%v)")
			vals = append(vals, equalityRange)
		}
	} else {
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

func str2id(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func nonOptimizedIdCond(d dialect, field string, values []string) ([]string, []interface{}, error) {
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

	if len(equality) == 1 {
		conds = append(conds, field+" = %v")
		vals = append(vals, equality[0])
	} else if len(equality) > 1 {
		conds = append(conds, field+" IN (%v)")
		vals = append(vals, equality)
	}

	return conds, vals, nil
}

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

func uuidCond() filterFunction {
	var fn = func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error) {
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

// not allowed for multiple conditions
func stringCond(maxValueLen int, allowLikes bool) filterFunction {
	var fn = func(d dialect, optimizeConditions bool, field string, values []string) ([]string, []interface{}, error) {
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

		for _, v := range values {
			fnc, val, err := procValue(field, v)
			if err != nil {
				return nil, nil, err
			}

			switch fnc {
			case "":
				positiveFilter = true
				valuesPositive = append(valuesPositive, val)
			case "ne":
				valuesNegative = append(valuesNegative, val)
			case "hlike":
				if !allowLikes {
					return nil, nil, fmt.Errorf("like functions are unsupported on field '%v'", field)
				}
				conds = append(conds, field+" LIKE %v")
				vals = append(vals, val+"%%")
			case "tlike":
				if !allowLikes {
					return nil, nil, fmt.Errorf("like functions are unsupported on field '%v'", field)
				}
				conds = append(conds, field+" LIKE %v")
				vals = append(vals, "%%"+val)
			case "like":
				if !allowLikes {
					return nil, nil, fmt.Errorf("like functions are unsupported on field '%v'", field)
				}
				conds = append(conds, field+" LIKE %v")
				vals = append(vals, "%%"+val+"%%")
			case "lt":
				conds = append(conds, field+" < %v")
				vals = append(vals, val)
			case "le":
				conds = append(conds, field+" <= %v")
				vals = append(vals, val)
			case "gt":
				conds = append(conds, field+" > %v")
				vals = append(vals, val)
			case "ge":
				conds = append(conds, field+" >= %v")
				vals = append(vals, val)
			default:
				return nil, nil, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, field)
			}
		}

		var valuesNegativeSet = map[string]struct{}{}
		for _, v := range valuesNegative {
			valuesNegativeSet[v] = struct{}{}
		}

		for _, v := range valuesPositive {
			if _, ok := valuesNegativeSet[v]; ok {
				return nil, nil, fmt.Errorf("positive condition on value cannot be set alongside with ne function on it, field '%v', value '%v'", field, v)
			}
		}

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
	var equalityRange, empty, err = optimize.EnumStringCond(field, values, conv, enumSize, enumValues)
	if err != nil {
		return nil, nil, err
	}

	if empty {
		return []string{}, []interface{}{}, nil
	}

	var conds []string
	var vals []interface{}

	switch len(equalityRange) {
	case 0:
	case 1:
		conds = append(conds, field+" = %v")
		vals = append(vals, equalityRange[0])

	default:
		conds = append(conds, field+" IN (%v)")
		vals = append(vals, equalityRange)
	}

	return conds, vals, nil
}

func nonOptimizedEnumStringCond(d dialect, conv func(string) (int64, error), enumSize int, enumValues []int64, field string, values []string) ([]string, []interface{}, error) {
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

	dvalues := db.DedupStrings(values)
	if len(dvalues) == 0 {
		return nil, nil, nil
	}

	var vals []interface{}
	var equality []int64
	var conds []string
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
	for _, v := range values {
		fnc, val, err := db.ParseFunc(v)
		if err != nil {
			return nil, nil, fmt.Errorf("%v on field '%v'", err, field)
		}

		var t time.Time
		t, err = db.ParseTimeUTC(val)
		if err != nil {
			return nil, nil, fmt.Errorf("%v on field '%v'", err, field)
		}

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

func (g *sqlGateway) Search(tableName string, sc *db.SelectCtrl) (db.Rows, error) {
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

	var rows *sql.Rows
	rows, err = g.rw.queryContext(g.ctx, query)

	return &sqlRows{rows: rows}, nil
}
