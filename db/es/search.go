package es

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/acronis/perfkit/db"
	"github.com/acronis/perfkit/db/optimize"
)

const (
	timeStoreFormatPrecise = time.RFC3339Nano
)

var indexQueryBuilders = make(map[indexName]searchQueryBuilder)

func createSearchQueryBuilder(idxName string, tableRows []db.TableRow) error {
	if _, ok := indexQueryBuilders[indexName(idxName)]; ok {
		return nil
	}

	var queryBuilder = searchQueryBuilder{
		queryable: make(map[string]filterFunction),
	}

	for _, row := range tableRows {
		switch row.Type {
		case db.DataTypeId:
			queryBuilder.queryable[row.Name] = idCond()
		case db.DataTypeUUID:
			queryBuilder.queryable[row.Name] = uuidCond()
		case db.DataTypeString:
			queryBuilder.queryable[row.Name] = stringCond(256, true)
		case db.DataTypeDateTime:
			queryBuilder.queryable[row.Name] = timeCond()
		}
	}

	indexQueryBuilders[indexName(idxName)] = queryBuilder

	return nil
}

var elasticRangeMapping = map[string]string{
	"ge": "gte",
	"le": "lte",
}

func elasticRange(old string) string {
	if nu, ok := elasticRangeMapping[old]; ok {
		return nu
	}
	return old
}

type filterFunction func(optimizeConditions bool, colField string, values []string) (*conditions, bool, error)

type searchQueryBuilder struct {
	queryable map[string]filterFunction
}

type selectorType string

const (
	selectorTerms   selectorType = "terms"
	selectorExists  selectorType = "exists"
	selectorRegexp  selectorType = "regexp"
	selectorRange   selectorType = "range"
	selectorShould  selectorType = "should"
	selectorBool    selectorType = "bool"
	selectorMustNot selectorType = "must_not"
)

type selector map[selectorType]map[string]interface{}

func newSelector(selType selectorType, fieldName string, value interface{}) selector {
	return selector{
		selType: {
			fieldName: value,
		},
	}
}

func newMustNotExistsSelector(fieldName string) selector {
	return selector{
		selectorBool: {
			string(selectorMustNot): newSelector(selectorExists, "field", fieldName),
		},
	}
}

func newShouldSelector(fieldName string, value interface{}) selector {
	return selector{
		selectorBool: {
			string(selectorShould): []selector{
				newSelector(selectorRange, fieldName, value),
				newMustNotExistsSelector(fieldName),
			},
		},
	}
}

type selectors []selector

func (sl *selectors) addExistsSelector(field string) {
	*sl = append(*sl, newSelector(selectorExists, "field", field))
}

func (sl *selectors) addRegexSelector(field string, value interface{}) {
	*sl = append(*sl, newSelector(selectorRegexp, field, value))
}

func (sl *selectors) addTermsSelector(field string, terms []interface{}) {
	*sl = append(*sl, newSelector(selectorTerms, field, terms))
}

func (sl *selectors) addRangeSelector(field string, ranges map[string]interface{}) {
	*sl = append(*sl, newSelector(selectorRange, field, ranges))
}

func (sl *selectors) addShouldSelector(field string, ranges map[string]interface{}) {
	*sl = append(*sl, newShouldSelector(field, ranges))
}

type conditions struct {
	Filter  selectors `json:"filter,omitempty"`
	MustNot selectors `json:"must_not,omitempty"`
}

func (c *conditions) Merge(other *conditions) {
	if c == nil || other == nil {
		return
	}

	if other.Filter != nil && len(other.Filter) > 0 {
		c.Filter = append(c.Filter, other.Filter...)
	}
	if other.MustNot != nil && len(other.MustNot) > 0 {
		c.MustNot = append(c.MustNot, other.MustNot...)
	}
}

func (c *conditions) isEmpty() bool {
	if c == nil {
		return true
	}

	return (c.Filter == nil || len(c.Filter) == 0) &&
		(c.MustNot == nil || len(c.MustNot) == 0)
}

type SearchQuery struct {
	MatchAll *struct{} `json:"match_all,omitempty"`

	Conditions *conditions `json:"bool,omitempty"`
}

type KnnRequest struct {
	Field       string    `json:"field"`
	QueryVector []float64 `json:"query_vector"`
}

type SearchRequest struct {
	Source bool                         `json:"_source"` // Should always be false
	Fields []string                     `json:"fields"`
	Query  *SearchQuery                 `json:"query"`
	Knn    *KnnRequest                  `json:"knn,omitempty"`
	Sort   []map[string]json.RawMessage `json:"sort,omitempty"` // list to keep ordering
	Size   int64                        `json:"size,omitempty"`
	From   int64                        `json:"from,omitempty"`
}

func (r *SearchRequest) String() string {
	b, err := json.MarshalIndent(r, "", "   ")
	if err != nil {
		return "{}"
	}
	return string(b)
}

type CountRequest struct {
	Query *SearchQuery `json:"query"`
}

func (r *CountRequest) String() string {
	b, err := json.MarshalIndent(r, "", "   ")
	if err != nil {
		return "{}"
	}
	return string(b)
}

type SearchHit struct {
	ID     string                 `json:"_id"`
	Index  string                 `json:"_index"`
	Score  float64                `json:"_score"`
	Fields map[string]interface{} `json:"fields"`
}

type ShardResult struct {
	Failed     int64 `json:"failed"`
	Skipped    int64 `json:"skipped,omitempty"`
	Successful int64 `json:"successful"`
	Total      int64 `json:"total"`
}

type SearchResponse struct {
	Count int64 `json:"count"`

	Shards ShardResult `json:"_shards"`

	Hits struct {
		Hits []SearchHit `json:"hits"`
	} `json:"hits"`
	MaxScore float64 `json:"max_score"`
	Total    struct {
		Relation string `json:"relation"`
		Value    int64  `json:"value"`
	}
	TimedOut bool  `json:"timed_out"`
	Took     int64 `json:"took"`
}

type queryType int

const (
	queryTypeError  queryType = 0
	queryTypeSearch queryType = 1
	queryTypeCount  queryType = 2
	queryTypeAggs   queryType = 3
)

func (b searchQueryBuilder) searchRequest(c *db.SelectCtrl) (*SearchRequest, queryType, bool, error) {
	if c == nil {
		return nil, queryTypeSearch, true, nil
	}
	var (
		q     *SearchQuery
		err   error
		empty bool
	)
	if c.Fields == nil {
		c.Fields = []string{}
	}
	q, empty, err = b.searchQuery(c.OptimizeConditions, c.Where)
	if err != nil {
		return nil, queryTypeError, false, fmt.Errorf("failed to create search request: %v", err)
	}

	if empty {
		return nil, queryTypeSearch, true, nil
	}

	req := &SearchRequest{
		Size:   c.Page.Limit,
		From:   c.Page.Offset,
		Source: false,
		Query:  q,
	}

	var count bool
	if req.Fields, count, err = b.fields(c.Fields); err != nil {
		return nil, queryTypeSearch, false, fmt.Errorf("failed to set request fields: %v", err)
	} else if count {
		return req, queryTypeCount, false, nil
	}

	if req.Sort, req.Knn, err = b.order(c.Order); err != nil {
		return nil, queryTypeSearch, false, fmt.Errorf("failed to parse order fields for request: %v", err)
	}

	return req, queryTypeSearch, false, nil
}

func (b searchQueryBuilder) searchQuery(optimizeConditions bool, filter map[string][]string) (*SearchQuery, bool, error) {
	var q = SearchQuery{}
	var err error
	var empty bool
	if q.Conditions, empty, err = b.filter(optimizeConditions, filter); err != nil {
		return nil, false, fmt.Errorf("failed to parse filter fields for request: %v", err)
	}

	if empty {
		return nil, true, nil
	}

	if q.Conditions.isEmpty() {
		q.Conditions = nil
		q.MatchAll = &struct{}{}
	}

	return &q, false, nil
}

func optimizedIdCond(field string, values []string) (*conditions, bool, error) {
	var minVal, maxVal, equalityRange, empty, err = optimize.IDCond(field, values)
	if err != nil {
		return nil, false, err
	}

	if empty {
		return nil, true, err
	}

	var bl = conditions{}

	if len(equalityRange) != 0 {
		var terms = make([]interface{}, 0)
		for _, value := range equalityRange {
			terms = append(terms, value)
		}

		bl.Filter.addTermsSelector(field, terms)
	} else {
		var ranges = make(map[string]interface{})

		var minInt = int64(math.MinInt64)
		var maxInt = int64(math.MaxInt64)

		if minVal != minInt {
			ranges["gt"] = minVal
		}
		if maxVal != maxInt {
			ranges["lt"] = maxVal
		}

		bl.Filter.addRangeSelector(field, ranges)
	}

	return &bl, false, nil
}

func str2id(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func nonOptimizedIdCond(field string, values []string) (*conditions, bool, error) {
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

	var bl = conditions{}
	var terms = make([]interface{}, 0)
	for _, v := range values {
		fnc, id, err := procVal(v)
		if err != nil {
			return nil, false, err
		}

		switch fnc {
		case "":
			terms = append(terms, id)
		case "lt", "le", "gt", "ge":
			var ranges = make(map[string]interface{})
			ranges[elasticRange(fnc)] = id
			bl.Filter.addRangeSelector(field, ranges)
		default:
			return nil, false, fmt.Errorf("unsupported function '%v' on field uuid", fnc)
		}
	}

	if len(terms) != 0 {
		bl.Filter.addTermsSelector(field, terms)
	}

	return &bl, false, nil
}

func idCond() filterFunction {
	var fn = func(optimizeConditions bool, field string, values []string) (*conditions, bool, error) {
		if optimizeConditions {
			return optimizedIdCond(field, values)
		} else {
			return nonOptimizedIdCond(field, values)
		}
	}

	return fn
}

func uuidCond() filterFunction {
	var fn = func(optimizeConditions bool, field string, values []string) (*conditions, bool, error) {
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

		var bl = conditions{}
		var terms = make([]interface{}, 0)
		for _, v := range values {
			fnc, id, err := procVal(v)
			if err != nil {
				return nil, false, err
			}

			switch fnc {
			case "":
				terms = append(terms, id)
			case "lt", "le", "gt", "ge":
				bl.Filter.addRangeSelector(field, map[string]interface{}{elasticRange(fnc): id})
			default:
				return nil, false, fmt.Errorf("unsupported function '%v' on field %s", fnc, field)
			}
		}

		if len(terms) != 0 {
			if field == "tenant_vis_list" {
				field = "tenant_vis_list.keyword"
			}
			bl.Filter.addTermsSelector(field, terms)
		}

		return &bl, false, nil
	}
	return fn
}

func stringCond(maxValueLen int, allowLikes bool) filterFunction {
	fn := func(optimizeConditions bool, colField string, values []string) (*conditions, bool, error) {
		var bl = conditions{}
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

		// Specification of translation API filters to SQL conditions:
		// 1. field  = value1 && field  = value2  ->  field IN (value1, value2)
		// 2. field != value1 && field != value2  ->  field NOT IN (value1, value2)
		// 3. field  = value1 && field != value2  ->  field IN (value1)
		// 4. field  = value1 && field != value1  ->  error
		var positiveFilter = false
		var valuesPositive []string
		var valuesNegative []string

		for _, v := range values {
			fnc, val, err := procValue(colField, v)
			if err != nil {
				return nil, false, err
			}

			switch fnc {
			case "":
				positiveFilter = true
				valuesPositive = append(valuesPositive, val)
			case "ne":
				valuesNegative = append(valuesNegative, val)
			case "hlike":
				if !allowLikes {
					return nil, false, fmt.Errorf("like functions are unsupported on field '%v'", colField)
				}
				bl.Filter.addRegexSelector(colField, val+".*")
			case "tlike":
				if !allowLikes {
					return nil, false, fmt.Errorf("like functions are unsupported on field '%v'", colField)
				}
				bl.Filter.addRegexSelector(colField, ".*"+val)
			case "like":
				if !allowLikes {
					return nil, false, fmt.Errorf("like functions are unsupported on field '%v'", colField)
				}
				bl.Filter.addRegexSelector(colField, ".*"+val+".*")
			case "lt", "le", "gt", "ge":
				bl.Filter.addRangeSelector(colField, map[string]interface{}{elasticRange(fnc): val})
			default:
				return nil, false, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, colField)
			}
		}

		var valuesNegativeSet = map[string]struct{}{}
		for _, v := range valuesNegative {
			valuesNegativeSet[v] = struct{}{}
		}

		for _, v := range valuesPositive {
			if _, ok := valuesNegativeSet[v]; ok {
				return nil, false, fmt.Errorf("positive condition on value cannot be set alongside with ne function on it, field '%v', value '%v'", colField, v)
			}
		}

		var valuesToAdd []interface{}
		if len(valuesPositive) > 0 || len(valuesNegative) > 0 {
			if positiveFilter {
				for _, v := range valuesPositive {
					valuesToAdd = append(valuesToAdd, v)
				}
				if len(valuesToAdd) != 0 {
					bl.Filter.addTermsSelector(colField, valuesToAdd)
				}
				if !optimizeConditions {
					if len(valuesNegative) != 0 {
						var negativeValuesToAdd []interface{}
						for _, v := range valuesNegative {
							negativeValuesToAdd = append(negativeValuesToAdd, v)
						}
						bl.MustNot.addTermsSelector(colField, negativeValuesToAdd)
					}
				}
			} else {
				for _, v := range valuesNegative {
					valuesToAdd = append(valuesToAdd, v)
				}
				if len(valuesToAdd) != 0 {
					bl.MustNot.addTermsSelector(colField, valuesToAdd)
				}
			}
		}

		return &bl, false, nil
	}
	return fn
}

func optimizedEnumStringCond(conv func(string) (int64, error), enumSize int, enumValues []int64, field string, values []string) (*conditions, bool, error) {
	var equalityRange, empty, err = optimize.EnumStringCond(field, values, conv, enumSize, enumValues)
	if err != nil {
		return nil, false, err
	}

	if empty {
		return nil, true, nil
	} else if len(equalityRange) == 0 {
		return nil, false, nil
	}

	var bl = conditions{}
	var terms = make([]interface{}, 0)
	for _, v := range equalityRange {
		terms = append(terms, v)
	}
	bl.Filter.addTermsSelector(field, terms)

	return &bl, false, nil
}

func nonOptimizedEnumStringCond(conv func(string) (int64, error), enumSize int, enumValues []int64, field string, values []string) (*conditions, bool, error) {
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
		return nil, true, nil
	}

	var bl = conditions{}
	var terms = make([]interface{}, 0)
	for _, v := range values {
		fnc, id, err := procVal(v)
		if err != nil {
			return nil, false, err
		}

		switch fnc {
		case "":
			terms = append(terms, id)
		case "lt", "le", "gt", "ge":
			bl.Filter.addRangeSelector(field, map[string]interface{}{elasticRange(fnc): id})
		default:
			return nil, false, fmt.Errorf("unsupported function '%v' on field %s", fnc, field)
		}
	}

	if len(terms) != 0 {
		bl.Filter.addTermsSelector(field, terms)
	}

	return &bl, false, nil
}

func enumStringCond(conv func(string) (int64, error), enumSize int, enumValues []int64) filterFunction {
	var fn = func(optimizeConditions bool, field string, values []string) (*conditions, bool, error) {
		if optimizeConditions {
			return optimizedEnumStringCond(conv, enumSize, enumValues, field, values)
		} else {
			return nonOptimizedEnumStringCond(conv, enumSize, enumValues, field, values)
		}
	}

	return fn
}

func optimizedTimeCond(colField string, values []string) (*conditions, bool, error) {
	var minVal, maxVal, equalityRange, empty, err = optimize.TimeCond(colField, values)
	if err != nil {
		return nil, false, err
	}

	if empty {
		return nil, true, nil
	}

	var bl = conditions{}

	var ranges = make(map[string]interface{})
	if len(equalityRange) != 0 {
		for _, value := range equalityRange {
			ranges["gt"] = value.Add(-1).Format(timeStoreFormatPrecise)
			ranges["lt"] = value.Add(1).Format(timeStoreFormatPrecise)
		}
		if len(equalityRange) == 1 {
			bl.Filter.addRangeSelector(colField, ranges)
		} else {
			bl.MustNot.addExistsSelector(colField)
		}

		return &bl, false, nil
	} else {
		var minTime = time.Unix(-2208988800, 0) // Jan 1, 1900
		var maxTime = time.Unix(1<<63-62135596801, 999999999)

		if maxVal != maxTime {
			ranges["lt"] = maxVal.Format(timeStoreFormatPrecise)
		}
		if minVal != minTime {
			ranges["gt"] = minVal.Format(timeStoreFormatPrecise)
		}

		bl.Filter.addRangeSelector(colField, ranges)
	}

	return &bl, false, nil
}

func nonOptimizedTimeCond(colField string, values []string) (*conditions, bool, error) {
	var bl = conditions{}
	if len(values) == 0 {
		return nil, true, nil
	}

	for _, v := range values {
		fnc, val, err := db.ParseFunc(v)
		if err != nil {
			return nil, false, fmt.Errorf("%v on field '%v'", err, colField)
		}

		var value time.Time
		value, err = db.ParseTimeUTC(val)
		if err != nil {
			return nil, false, fmt.Errorf("%v on field '%v'", err, colField)
		}

		switch fnc {
		case "":
			var ranges = make(map[string]interface{})
			ranges["gt"] = value.Add(-1).Format(timeStoreFormatPrecise)
			ranges["lt"] = value.Add(1).Format(timeStoreFormatPrecise)
			bl.Filter.addRangeSelector(colField, ranges)
		case "lt", "gt":
			bl.Filter.addRangeSelector(colField, map[string]interface{}{elasticRange(fnc): value.Format(timeStoreFormatPrecise)})
		case "le":
			bl.Filter.addRangeSelector(colField, map[string]interface{}{"lt": value.Add(1).Format(timeStoreFormatPrecise)})
		case "ge":
			bl.Filter.addRangeSelector(colField, map[string]interface{}{"gt": value.Add(-1).Format(timeStoreFormatPrecise)})
		default:
			return nil, false, fmt.Errorf("unsupported function '%v' on field '%s'", fnc, colField)
		}
	}

	return &bl, false, nil
}

func timeCond() filterFunction {
	var fn = func(optimizeConditions bool, colField string, values []string) (*conditions, bool, error) {
		if optimizeConditions {
			return optimizedTimeCond(colField, values)
		} else {
			return nonOptimizedTimeCond(colField, values)
		}
	}

	return fn
}

func orderAsc() json.RawMessage {
	return json.RawMessage(`{"order":"asc", "missing": "_first"}`)
}

func orderDesc() json.RawMessage {
	return json.RawMessage(`{"order":"desc", "missing": "_last"}`)
}

func orderBy(fnc string) (json.RawMessage, error) {
	var qry json.RawMessage
	switch fnc {
	case "asc":
		qry = orderAsc()
	case "desc":
		qry = orderDesc()
	default:
		return nil, fmt.Errorf("bad order function '%v'", fnc)
	}
	return qry, nil
}

func (b searchQueryBuilder) order(sortFields []string) ([]map[string]json.RawMessage, *KnnRequest, error) {
	var orderQuery []map[string]json.RawMessage
	var knn *KnnRequest

	for _, value := range sortFields {
		fnc, args, err := db.ParseFuncMultipleArgs(value, ";")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse order function: %v", err)
		}

		if len(args) == 0 {
			return nil, nil, fmt.Errorf("empty order field")
		}

		switch fnc {
		case "asc", "desc":
			if len(args) != 1 {
				return nil, nil, fmt.Errorf("number of args %d doesn't match number of conditions 1", len(args))
			}

			var qry json.RawMessage
			if fnc == "asc" {
				qry = orderAsc()
			} else {
				qry = orderDesc()
			}

			orderQuery = append(orderQuery, map[string]json.RawMessage{
				args[0]: qry,
			})

		case "nearest":
			if len(args) != 3 {
				return nil, nil, fmt.Errorf("number of args %d doesn't match number of conditions for nearest function, should be 3", len(args))
			}

			var field = args[0]
			var rawVector []string
			if rawVector, err = db.ParseVector(args[2], ","); err != nil {
				return nil, nil, fmt.Errorf("failed to parse vector: %v", err)
			}

			var vector []float64
			for _, v := range rawVector {
				var f float64
				v = strings.TrimSpace(v) // Removes any leading/trailing whitespace
				f, err = strconv.ParseFloat(v, 64)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to parse vector value: %v", err)
				}
				vector = append(vector, f)
			}

			if knn == nil {
				knn = &KnnRequest{
					Field:       field,
					QueryVector: vector,
				}
			} else {
				return nil, nil, fmt.Errorf("knn function already set")
			}
		default:
			return nil, nil, fmt.Errorf("bad order function '%v'", fnc)
		}
	}

	if len(orderQuery) != 0 && knn != nil {
		return nil, nil, fmt.Errorf("orderQuery and knn functions are mutually exclusive")
	}

	return orderQuery, knn, nil
}

func (b searchQueryBuilder) fields(selectedFields []string) ([]string, bool, error) {
	if len(selectedFields) == 0 {
		return []string{}, false, nil
	}

	if len(selectedFields) == 1 && selectedFields[0] == "COUNT(0)" {
		return []string{}, true, nil
	}

	var result []string

	switch {
	default:
		for _, fld := range selectedFields {
			if fld == "" {
				return nil, false, fmt.Errorf("empty request field")
			}

			result = append(result, fld)
		}
	}

	return result, false, nil
}

func (b searchQueryBuilder) filter(optimizeConditions bool, filterFields map[string][]string) (*conditions, bool, error) {
	var bl = conditions{}

	if filterFields == nil {
		return &bl, false, nil
	}

	for _, c := range db.SortFields(filterFields) {
		if c.Col == "" {
			return nil, false, fmt.Errorf("empty condition field")
		}
		var condgen, ok = b.queryable[c.Col]
		if !ok {
			return nil, false, fmt.Errorf("bad condition field '%v'", c.Col)
		}

		if len(c.Vals) == 1 {
			// generic special cases
			if c.Vals[0] == db.SpecialConditionIsNotNull {
				bl.Filter.addExistsSelector(c.Col)
				continue
			}
			if c.Vals[0] == db.SpecialConditionIsNull {
				bl.MustNot.addExistsSelector(c.Col)
				continue
			}
		}

		var newFilter, empty, err = condgen(optimizeConditions, c.Col, c.Vals)
		if err != nil {
			return nil, false, fmt.Errorf("failed to generate condition for column %s: %v", c.Col, err)
		}

		if empty {
			return nil, true, nil
		}

		bl.Merge(newFilter)
	}
	return &bl, false, nil
}

func (g *esGateway) Select(idxName string, sc *db.SelectCtrl) (db.Rows, error) {
	var index = indexName(idxName)

	var queryBuilder, ok = indexQueryBuilders[index]
	if !ok {
		return nil, fmt.Errorf("index %s is not supported", index)
	}

	var query, qType, empty, err = queryBuilder.searchRequest(sc)
	if err != nil {
		return nil, err
	}

	if empty {
		return &db.EmptyRows{}, nil
	}

	switch qType {
	case queryTypeSearch:
		var fields []map[string]interface{}
		if fields, err = g.q.search(g.ctx.Ctx, index, query); err != nil {
			return nil, fmt.Errorf("failed to search: %v", err)
		}

		if len(fields) == 0 {
			return &db.EmptyRows{}, nil
		}

		return &esRows{data: fields, requestedColumns: sc.Fields}, nil
	case queryTypeCount:
		var countQuery = &CountRequest{
			Query: query.Query,
		}

		var count int64
		if count, err = g.q.count(g.ctx.Ctx, index, countQuery); err != nil {
			return nil, fmt.Errorf("failed to count: %v", err)
		}

		return &db.CountRows{Count: count}, nil
	default:
		return nil, fmt.Errorf("unsupported query type %v", qType)
	}
}
