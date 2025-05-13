package es

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/acronis/perfkit/db"
)

var testQueryBuilder searchQueryBuilder

func init() {
	testQueryBuilder.queryable = map[string]filterFunction{
		"id":              idCond(),
		"uuid":            uuidCond(),
		"type":            stringCond(64, false),
		"policy_name":     stringCond(256, true), // +like,+hlike,+tlike
		"resource_name":   stringCond(256, true), // +like,+hlike,+tlike
		"start_time":      timeCond(),
		"tenant_vis_list": uuidCond(),
	}
}

func jsonBytesToMap(t *testing.T, data []byte) map[string]interface{} {
	m := make(map[string]interface{})
	err := json.Unmarshal(data, &m) // shouldn't ever fail because we are passing bytes which are the result of prior Marshaling
	require.NoError(t, err, "failed to unmarshal json bytes to map")
	return m
}

func getExpectedActual(expected, actual *SearchRequest) ([]byte, []byte, error) {
	var exp, err = json.Marshal(expected)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal expected: %v", err)
	}

	act, err := json.Marshal(actual)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal actual: %v", err)
	}
	return exp, act, nil
}

func TestSearchQueryBuilder(t *testing.T) {
	type testSearchQuery = struct {
		c *db.SelectCtrl

		request *SearchRequest
		empty   bool
		err     error
	}

	var tests = []testSearchQuery{
		{
			c: nil,

			empty: true,
		},
		{
			c: &db.SelectCtrl{Order: []string{"desc(id)", "asc(uuid)"}},

			request: &SearchRequest{
				Source: false,
				Fields: []string{},
				Query:  &SearchQuery{MatchAll: &struct{}{}},
				Sort: []map[string]json.RawMessage{
					{"id": orderDesc()},
					{"uuid": orderAsc()},
				},
			},
		},
		{
			c: &db.SelectCtrl{Fields: []string{"id", "uuid"}},

			request: &SearchRequest{
				Source: false,
				Fields: []string{"id", "uuid"},
				Query:  &SearchQuery{MatchAll: &struct{}{}},
			},
		},
		{
			c: &db.SelectCtrl{Where: map[string][]string{
				"id": {"1"},
			}},

			request: &SearchRequest{
				Source: false,
				Fields: []string{},
				Query: &SearchQuery{
					Conditions: &conditions{
						Filter: selectors{
							selector{selectorTerms: {"id": []interface{}{int64(1)}}},
						},
					},
				},
			},
		},
		/*
			{
				c: &db.SelectCtrl{Where: map[string][]string{
					"slice": {"one", "two"},
				}},

				request: &SearchRequest{
					Source: false,
					Fields: []string{},
					Query: &SearchQuery{
						Conditions: &conditions{
							Filter: selectors{
								selector{selectorTerms: {"slice": []interface{}{"one", "two"}}},
							},
						},
					},
				},
			},

		*/
		{
			c: &db.SelectCtrl{Where: map[string][]string{"id": {"1"}}},

			request: &SearchRequest{
				Source: false,
				Fields: []string{},
				Query: &SearchQuery{
					Conditions: &conditions{
						Filter: selectors{
							selector{selectorTerms: {"id": []interface{}{int64(1)}}},
						},
					},
				},
			},
		},
		{
			c: &db.SelectCtrl{Where: map[string][]string{"id": {"1"}, "uuid": {"00000000-0000-0000-0000-000000000001"}}},

			request: &SearchRequest{
				Source: false,
				Fields: []string{},
				Query: &SearchQuery{
					Conditions: &conditions{
						Filter: selectors{
							selector{
								selectorTerms: {
									"id": []interface{}{int64(1)},
								},
							},
							selector{
								selectorTerms: {
									"uuid": []interface{}{"00000000-0000-0000-0000-000000000001"},
								},
							},
						},
					},
				},
			},
		},
		{
			c: &db.SelectCtrl{
				Fields: []string{"id", "uuid"},
				Order:  []string{"desc(id)", "asc(uuid)"},
				Page:   db.Page{},
				Where:  map[string][]string{"id": {"1", "2"}, "uuid": {"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"}},
			},

			request: &SearchRequest{
				Source: false,
				Fields: []string{"id", "uuid"},
				Query: &SearchQuery{
					Conditions: &conditions{
						Filter: selectors{
							selector{
								selectorTerms: {
									"id": []interface{}{int64(1), int64(2)},
								},
							},
							selector{
								selectorTerms: {
									"uuid": []interface{}{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},
								},
							},
						},
					},
				},
				Sort: []map[string]json.RawMessage{
					{"id": orderDesc()},
					{"uuid": orderAsc()},
				},
			},
		},
		{
			c: &db.SelectCtrl{
				Fields: []string{"id", "type"},
				Order:  []string{"asc(id)"},
				Where:  map[string][]string{"start_time": {fmt.Sprintf("ge(%d)", int64(10000))}},
			},

			request: &SearchRequest{
				Source: false,
				Fields: []string{"id", "type"},
				Query: &SearchQuery{
					Conditions: &conditions{
						Filter: selectors{
							selector{
								selectorRange: {
									"start_time": map[string]interface{}{"gt": time.Unix(int64(10000), 0).UTC().Add(-1).Format(timeStoreFormatPrecise)},
								},
							},
						},
					},
				},
				Sort: []map[string]json.RawMessage{
					{"id": orderAsc()},
				},
			},
		},
		{
			c: &db.SelectCtrl{
				Fields: []string{"id", "type"},
				Order:  []string{"asc(id)"},
				Where:  map[string][]string{"start_time": {fmt.Sprintf("ge(%d)", int64(10000))}},
				Page: db.Page{
					Limit:  10,
					Offset: 20,
				},
			},
			request: &SearchRequest{
				Source: false,
				Fields: []string{"id", "type"},
				Query: &SearchQuery{
					Conditions: &conditions{
						Filter: selectors{
							selector{
								selectorRange: {
									"start_time": map[string]interface{}{"gt": time.Unix(int64(10000), 0).UTC().Add(-1).Format(timeStoreFormatPrecise)},
								},
							},
						},
					},
				},
				Sort: []map[string]json.RawMessage{
					{"id": orderAsc()},
				},
				Size: 10,
				From: 20,
			},
		},
		{
			c: &db.SelectCtrl{
				Fields: []string{"id", "type"},
				Order:  []string{"asc(id"},
				Where:  map[string][]string{"start_time": {fmt.Sprintf("ge(%d)", int64(10000))}},
				Page: db.Page{
					Limit:  10,
					Offset: 20,
				},
			},
			err: fmt.Errorf("failed to parse order fields for request: failed to parse order function: bad function 'asc(id', no closing bracket"),
		},
		{
			c: &db.SelectCtrl{
				Fields: []string{"type"},
				Order:  []string{"asc(id)"},
				Where:  map[string][]string{"start_time": {fmt.Sprintf("ge%d)", int64(10000))}},
				Page: db.Page{
					Limit:  10,
					Offset: 20,
				},
			},
			err: fmt.Errorf("failed to create search request: failed to parse filter fields for request: failed to generate condition for column start_time: bad function 'ge10000)', no opening bracket on field 'start_time'"),
		},
		{
			c: &db.SelectCtrl{
				Fields: []string{"id", "type"},
				Order:  []string{"desc(enqueue_time)"},
				Where:  map[string][]string{"tenant_vis_list": {"00000000-0000-0000-0000-000000000001"}},
				Page: db.Page{
					Limit: 30,
				},
			},
			request: &SearchRequest{
				Source: false,
				Fields: []string{"id", "type"},
				Query: &SearchQuery{
					Conditions: &conditions{
						Filter: selectors{
							selector{
								selectorTerms: {
									"tenant_vis_list.keyword": []interface{}{"00000000-0000-0000-0000-000000000001"},
								},
							},
						},
					},
				},
				Sort: []map[string]json.RawMessage{
					{"enqueue_time": orderDesc()},
				},
				Size: 30,
			},
		},
	}

	for _, test := range tests {
		var query, _, empty, err = testQueryBuilder.searchRequest("", test.c)
		if err != nil {
			if test.err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for col %v", test.c)
			} else {
				t.Fatalf("unexpected error: %v", err)
			}

			continue
		}

		assert.Equalf(t, test.empty, empty, "wrong empty value in test for col %v", test.c)
		if empty {
			continue
		}

		t.Log(query)

		var expected, actual, formatErr = getExpectedActual(test.request, query)
		assert.NoError(t, formatErr, "failed to format expected and actual queries")
		assert.Equalf(t, jsonBytesToMap(t, expected), jsonBytesToMap(t, actual), "wrong empty value in test for col %v", test.c)
	}
}

func TestOptimizedConditionId(t *testing.T) {
	type idCondTestCase = struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}

	var tests = []idCondTestCase{
		{
			values: []string{"1"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {"id": []interface{}{int64(1)}},
					},
				},
			},
		},
		{
			values: []string{"1", "2", "4"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {"id": []interface{}{int64(1), int64(2), int64(4)}},
					},
				},
			},
		},
		{
			values: []string{"le(1)", "2"},

			empty: true,
		},
		{
			values: []string{"ge(1)", "2"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {"id": []interface{}{int64(2)}},
					},
				},
			},
		},
		{
			values: []string{"gt(1)", "le(4)"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: {"id": map[string]interface{}{"gt": int64(1), "lt": int64(5)}},
					},
				},
			},
		},
		{
			values: []string{"not_an_id"},

			err: fmt.Errorf("strconv.ParseInt: parsing \"not_an_id\": invalid syntax on field 'id'"),
		},
		{
			values: []string{"4", "not_an_id"},

			err: fmt.Errorf("strconv.ParseInt: parsing \"not_an_id\": invalid syntax on field 'id'"),
		},
	}

	for _, test := range tests {
		var actual, empty, err = idCond()(true, "id", test.values)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
		if empty {
			continue
		}

		assert.Equalf(t, test.expected, actual, "failure in test for values %v", test.values)
	}
}

func TestNonOptimizedConditionId(t *testing.T) {
	type idCondTestCase = struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}

	var tests = []idCondTestCase{
		{
			values: []string{"1"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {"id": []interface{}{int64(1)}},
					},
				},
			},
		},
		{
			values: []string{"1", "2", "4"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {"id": []interface{}{int64(1), int64(2), int64(4)}},
					},
				},
			},
		},
		{
			values: []string{"le(1)", "2"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: {"id": map[string]interface{}{"lte": int64(1)}},
					},
					selector{
						selectorTerms: {"id": []interface{}{int64(2)}},
					},
				},
			},
		},
		{
			values: []string{"ge(1)", "2"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: {"id": map[string]interface{}{"gte": int64(1)}},
					},
					selector{
						selectorTerms: {"id": []interface{}{int64(2)}},
					},
				},
			},
		},
		{
			values: []string{"gt(1)", "le(4)"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: {"id": map[string]interface{}{"gt": int64(1)}},
					},
					selector{
						selectorRange: {"id": map[string]interface{}{"lte": int64(4)}},
					},
				},
			},
		},
		{
			values: []string{"not_an_id"},

			err: fmt.Errorf("strconv.ParseInt: parsing \"not_an_id\": invalid syntax on field 'id'"),
		},
		{
			values: []string{"4", "not_an_id"},

			err: fmt.Errorf("strconv.ParseInt: parsing \"not_an_id\": invalid syntax on field 'id'"),
		},
	}

	for _, test := range tests {
		var actual, empty, err = idCond()(false, "id", test.values)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
		if empty {
			continue
		}

		assert.Equalf(t, test.expected, actual, "failure in test for values %v", test.values)
	}
}

func TestUuidCond(t *testing.T) {
	type uuidCondTestCase struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}

	var tests = []uuidCondTestCase{
		{
			values: []string{"00000000-0000-0000-0000-000000000001"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {"uuid": []interface{}{"00000000-0000-0000-0000-000000000001"}},
					},
				},
			},
		},
		{
			values: []string{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {"uuid": []interface{}{"00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0000-000000000002"}},
					},
				},
			},
		},
		{
			values: []string{"le(00000000-0000-0000-0000-000000000001)"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: {"uuid": map[string]interface{}{"lte": "00000000-0000-0000-0000-000000000001"}},
					},
				},
			},
		},
		{
			values: []string{"gt(00000000-0000-0000-0000-000000000001)"},

			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: {"uuid": map[string]interface{}{"gt": "00000000-0000-0000-0000-000000000001"}},
					},
				},
			},
		},
		{
			values: []string{"not_a_uuid"},

			err: fmt.Errorf("bad uuid format: invalid UUID length: 10"),
		},
		{
			values: []string{"00000000-0000-0000-0000-000000000001", "not_an_id"},

			err: fmt.Errorf("bad uuid format: invalid UUID length: 9"),
		},
		{
			values: []string{"le(not_a_uuid"},

			err: fmt.Errorf("bad function 'le(not_a_uuid', no closing bracket on uuid field"),
		},
		{
			values: []string{"hex(00000000-0000-0000-0000-000000000001)"},

			err: fmt.Errorf("unsupported function 'hex' on field uuid"),
		},
	}

	for _, test := range tests {
		for _, optimized := range []bool{false, true} {
			var actual, empty, err = uuidCond()(optimized, "uuid", test.values)
			if err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
				continue
			}

			assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
			if empty {
				continue
			}

			assert.Equal(t, test.expected, actual, "failure in test for values %v", test.values)
		}
	}
}

func TestBasicStringCond(t *testing.T) {
	type stringCondTestCase struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}

	var defaultColumn = "type"
	var defaultMaxLength = 16

	var tests = []stringCondTestCase{
		{
			values: []string{"val1"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{"val1"},
						},
					},
				},
			},
		},
		{
			values: []string{"val1", "val2"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{"val1", "val2"},
						},
					},
				},
			},
		},
		{
			values: []string{"ne(val1)"},
			expected: &conditions{
				MustNot: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{"val1"},
						},
					},
				},
			},
		},
		{
			values:   []string{},
			expected: &conditions{},
		},
		{
			values: []string{"ne(val1"},
			err:    fmt.Errorf("bad function 'ne(val1', no closing bracket on field 'type'"),
		},
		{
			values: []string{"ne()"},
			err:    fmt.Errorf("field 'type' value is empty"),
		},
		{
			values: []string{"some_func(val1)"},
			err:    fmt.Errorf("unsupported function 'some_func' on field 'type'"),
		},
		{
			values: []string{"very_long_string_value_over_16_characters_long"},
			err:    fmt.Errorf("field 'type' value is too long, max length is 16"),
		},
		{
			values: []string{"val1", "ne(val1)"},
			err:    fmt.Errorf("positive condition on value cannot be set alongside with ne function on it, field 'type', value 'val1'"),
		},
		{
			values: []string{"hlike(val1)"},
			err:    fmt.Errorf("like functions are unsupported on field 'type'"),
		},
		{
			values: []string{"like(val1)"},
			err:    fmt.Errorf("like functions are unsupported on field 'type'"),
		},
		{
			values: []string{"tlike(val1)"},
			err:    fmt.Errorf("like functions are unsupported on field 'type'"),
		},
	}

	for _, test := range tests {
		for _, optimized := range []bool{false, true} {
			var actual, empty, err = stringCond(defaultMaxLength, false)(optimized, defaultColumn, test.values)
			if err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
				continue
			}

			assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
			if empty {
				continue
			}

			assert.Equal(t, test.expected, actual, "failure in test for values %v", test.values)
		}
	}
}

func TestRegexStringCond(t *testing.T) {
	type regexCondTestCase = struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}

	var defaultColumn = "resource_name"
	var defaultMaxLength = 16

	var tests = []regexCondTestCase{
		{
			values: []string{"hlike(val1)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRegexp: {
							defaultColumn: "val1.*",
						},
					},
				},
			},
		},
		{
			values: []string{"tlike(val1)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRegexp: {
							defaultColumn: ".*val1",
						},
					},
				},
			},
		},
		{
			values: []string{"like(val1)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRegexp: {
							defaultColumn: ".*val1.*",
						},
					},
				},
			},
		},
		{
			values: []string{"like(w234567890123456)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRegexp: {
							defaultColumn: ".*w234567890123456.*",
						},
					},
				},
			},
		},
		{
			values: []string{"like(w2345678901234567)"},
			err:    fmt.Errorf("field 'resource_name' value is too long, max length is 16"),
		},
	}

	for _, test := range tests {
		for _, optimized := range []bool{false, true} {
			var actual, empty, err = stringCond(defaultMaxLength, true)(optimized, defaultColumn, test.values)
			if err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
				continue
			}

			assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
			if empty {
				continue
			}

			assert.Equal(t, test.expected, actual, "failure in test for values %v", test.values)
		}
	}
}

func TestOptimizedEnumStringCond(t *testing.T) {
	type enumStringCondTestCase struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}

	var defaultColumn = "state"
	var defaultMaxValues = 4
	var enumConversionMap = map[string]int64{
		"val1": 1,
		"val2": 2,
		"val3": 3,
		"val4": 4,
	}

	var enumValues = []int64{1, 2, 3, 4}

	var convFunc = func(s string) (int64, error) {
		if v, ok := enumConversionMap[s]; ok {
			return v, nil
		}
		return 0, fmt.Errorf("invalid enum value: `%s`", s)
	}

	var tests = []enumStringCondTestCase{
		{
			values: []string{"val1"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{int64(1)},
						},
					},
				},
			},
		},
		{
			values: []string{"val1", "val2"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{int64(1), int64(2)},
						},
					},
				},
			},
		},
		{
			values: []string{"val1", "val2", "val1"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{int64(1), int64(2)},
						},
					},
				},
			},
		},
		{
			values:   []string{"val1", "val2", "val3", "val4"},
			expected: nil,
		},
		{
			values: []string{"val1", "val5"},
			err:    fmt.Errorf("invalid enum value: `val5` on field 'state'"),
		},
		{
			values: []string{"val1", "val2", "val3", "val5"},
			err:    fmt.Errorf("invalid enum value: `val5` on field 'state'"),
		},
		{
			values: []string{"val1", "val2", "val3", "val4", "val5"},
			err:    fmt.Errorf("invalid enum value: `val5` on field 'state'"),
		},
		{
			values: []string{""},
			err:    fmt.Errorf("invalid enum value: `` on field 'state'"),
		},
		{
			values: []string{},
			empty:  true,
		},
	}

	for _, test := range tests {
		var actual, empty, err = enumStringCond(convFunc, defaultMaxValues, enumValues)(true, defaultColumn, test.values)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
		if empty {
			continue
		}

		assert.Equal(t, test.expected, actual, "failure in test for values %v", test.values)
	}
}

func TestNonOptimizedEnumStringCond(t *testing.T) {
	type enumStringCondTestCase struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}

	var defaultColumn = "state"
	var defaultMaxValues = 4
	var enumConversionMap = map[string]int64{
		"val1": 1,
		"val2": 2,
		"val3": 3,
		"val4": 4,
	}

	var enumValues = []int64{1, 2, 3, 4}

	var convFunc = func(s string) (int64, error) {
		if v, ok := enumConversionMap[s]; ok {
			return v, nil
		}
		return 0, fmt.Errorf("invalid enum value: `%s`", s)
	}

	var tests = []enumStringCondTestCase{
		{
			values: []string{"val1"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{int64(1)},
						},
					},
				},
			},
		},
		{
			values: []string{"val1", "val2"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{int64(1), int64(2)},
						},
					},
				},
			},
		},
		{
			values: []string{"val1", "val2", "val1"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{int64(1), int64(2), int64(1)},
						},
					},
				},
			},
		},
		{
			values: []string{"val1", "val2", "val3", "val4"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorTerms: {
							defaultColumn: []interface{}{int64(1), int64(2), int64(3), int64(4)},
						},
					},
				},
			},
		},
		{
			values: []string{"val1", "val5"},
			err:    fmt.Errorf("invalid enum value: `val5` on field 'state'"),
		},
		{
			values: []string{"val1", "val2", "val3", "val5"},
			err:    fmt.Errorf("invalid enum value: `val5` on field 'state'"),
		},
		{
			values: []string{"val1", "val2", "val3", "val4", "val5"},
			err:    fmt.Errorf("invalid enum value: `val5` on field 'state'"),
		},
		{
			values: []string{""},
			err:    fmt.Errorf("invalid enum value: `` on field 'state'"),
		},
		{
			values: []string{},
			empty:  true,
		},
	}

	for _, test := range tests {
		var actual, empty, err = enumStringCond(convFunc, defaultMaxValues, enumValues)(false, defaultColumn, test.values)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
		if empty {
			continue
		}

		assert.Equal(t, test.expected, actual, "failure in test for values %v", test.values)
	}
}

func TestOptimizedTimeCond(t *testing.T) {
	type timeCondTestCase struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}
	var colName = "enqueue_time"

	var tests = []timeCondTestCase{
		{
			values: []string{"10000"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(10000, 0).UTC().Add(-1)).Format(timeStoreFormatPrecise),
								"lt": (time.Unix(10000, 0).UTC().Add(1)).Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"10000ns"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(0, 10000).UTC().Add(-1)).Format(timeStoreFormatPrecise),
								"lt": (time.Unix(0, 10000).UTC().Add(1)).Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"gt(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(10000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"lt(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"lt": (time.Unix(10000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"ge(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(10000, 0)).UTC().Add(-1).Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"le(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"lt": (time.Unix(10000, 0)).UTC().Add(1).Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"gt(10000)", "lt(20000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(10000, 0)).UTC().Format(timeStoreFormatPrecise),
								"lt": (time.Unix(20000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"gt(20000)", "lt(10000)"},
			empty:  true,
		},
		{
			values: []string{},
			empty:  true,
		},
		{
			values: []string{"le(10000"},
			err:    fmt.Errorf("bad function 'le(10000', no closing bracket on field 'enqueue_time'"),
		},
		{
			values: []string{"le10000)"},
			err:    fmt.Errorf("bad function 'le10000)', no opening bracket on field 'enqueue_time'"),
		},
		{
			values: []string{"le)10000("},
			err:    fmt.Errorf("bad function 'le)10000(', closing bracket placed before opening bracket on field 'enqueue_time'"),
		},
		{
			values: []string{"fake_func(10000)"},
			err:    fmt.Errorf("unsupported function 'fake_func' on field 'enqueue_time'"),
		},
		{
			values: []string{"gt(10000ms)"},
			err:    fmt.Errorf("incorrect time format, must be one of (UNIX-TIMESTAMP-NANO, UNIX-TIMESTAMP, RFC3339, RFC1123, RFC850, ANSI-C) on field 'enqueue_time'"),
		},
		{
			values: []string{"10000ms"},
			err:    fmt.Errorf("incorrect time format, must be one of (UNIX-TIMESTAMP-NANO, UNIX-TIMESTAMP, RFC3339, RFC1123, RFC850, ANSI-C) on field 'enqueue_time'"),
		},
		{
			values: []string{"1oo"},
			err:    fmt.Errorf("incorrect time format, must be one of (UNIX-TIMESTAMP-NANO, UNIX-TIMESTAMP, RFC3339, RFC1123, RFC850, ANSI-C) on field 'enqueue_time'"),
		},
		{
			values: []string{"le()"},
			err:    fmt.Errorf("empty time value on field 'enqueue_time'"),
		},
		{
			values: []string{""},
			err:    fmt.Errorf("empty time value on field 'enqueue_time'"),
		},
	}

	for _, test := range tests {
		var actual, empty, err = timeCond()(true, colName, test.values)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
		if empty {
			continue
		}

		assert.Equal(t, test.expected, actual, "failure in test for values %v", test.values)
	}
}

func TestNonOptimizedTimeCond(t *testing.T) {
	type timeCondTestCase struct {
		values []string

		expected *conditions
		empty    bool
		err      error
	}
	var colName = "enqueue_time"

	var tests = []timeCondTestCase{
		{
			values: []string{"10000"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(10000, 0).UTC().Add(-1)).Format(timeStoreFormatPrecise),
								"lt": (time.Unix(10000, 0).UTC().Add(1)).Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"10000ns"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(0, 10000).UTC().Add(-1)).Format(timeStoreFormatPrecise),
								"lt": (time.Unix(0, 10000).UTC().Add(1)).Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"gt(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(10000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"lt(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"lt": (time.Unix(10000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"ge(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(10000, 0)).UTC().Add(-1).Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"le(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"lt": (time.Unix(10000, 0)).UTC().Add(1).Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"gt(10000)", "lt(20000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(10000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"lt": (time.Unix(20000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{"gt(20000)", "lt(10000)"},
			expected: &conditions{
				Filter: selectors{
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"gt": (time.Unix(20000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
					selector{
						selectorRange: map[string]interface{}{
							colName: map[string]interface{}{
								"lt": (time.Unix(10000, 0)).UTC().Format(timeStoreFormatPrecise),
							},
						},
					},
				},
			},
		},
		{
			values: []string{},
			empty:  true,
		},
		{
			values: []string{"le(10000"},
			err:    fmt.Errorf("bad function 'le(10000', no closing bracket on field 'enqueue_time'"),
		},
		{
			values: []string{"le10000)"},
			err:    fmt.Errorf("bad function 'le10000)', no opening bracket on field 'enqueue_time'"),
		},
		{
			values: []string{"le)10000("},
			err:    fmt.Errorf("bad function 'le)10000(', closing bracket placed before opening bracket on field 'enqueue_time'"),
		},
		{
			values: []string{"fake_func(10000)"},
			err:    fmt.Errorf("unsupported function 'fake_func' on field 'enqueue_time'"),
		},
		{
			values: []string{"gt(10000ms)"},
			err:    fmt.Errorf("incorrect time format, must be one of (UNIX-TIMESTAMP-NANO, UNIX-TIMESTAMP, RFC3339, RFC1123, RFC850, ANSI-C) on field 'enqueue_time'"),
		},
		{
			values: []string{"10000ms"},
			err:    fmt.Errorf("incorrect time format, must be one of (UNIX-TIMESTAMP-NANO, UNIX-TIMESTAMP, RFC3339, RFC1123, RFC850, ANSI-C) on field 'enqueue_time'"),
		},
		{
			values: []string{"1oo"},
			err:    fmt.Errorf("incorrect time format, must be one of (UNIX-TIMESTAMP-NANO, UNIX-TIMESTAMP, RFC3339, RFC1123, RFC850, ANSI-C) on field 'enqueue_time'"),
		},
		{
			values: []string{"le()"},
			err:    fmt.Errorf("empty time value on field 'enqueue_time'"),
		},
		{
			values: []string{""},
			err:    fmt.Errorf("empty time value on field 'enqueue_time'"),
		},
	}

	for _, test := range tests {
		var actual, empty, err = timeCond()(false, colName, test.values)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
		if empty {
			continue
		}

		assert.Equal(t, test.expected, actual, "failure in test for values %v", test.values)
	}
}

func TestOrder(t *testing.T) {
	type orderTestCase struct {
		values []string

		expected []map[string]json.RawMessage
		err      error
	}
	var tests = []orderTestCase{
		{
			values: []string{"asc(id)", "desc(type)"},
			expected: []map[string]json.RawMessage{
				{"id": orderAsc()},
				{"type": orderDesc()},
			},
		},
		{
			values: []string{"bad_func(id)"},
			err:    fmt.Errorf("bad order function 'bad_func'"),
		},
		{
			values: []string{"id"},
			err:    fmt.Errorf("bad order function ''"),
		},
		{
			values: []string{"asc()"},
			err:    fmt.Errorf("empty order field"),
		},
		{
			values: []string{"desc)id("},
			err:    fmt.Errorf("failed to parse order function: bad function 'desc)id(', closing bracket placed before opening bracket"),
		},
		{
			values: []string{"desc(id"},
			err:    fmt.Errorf("failed to parse order function: bad function 'desc(id', no closing bracket"),
		},
		{
			values: []string{"desc)id)"},
			err:    fmt.Errorf("failed to parse order function: bad function 'desc)id)', no opening bracket"),
		},
	}

	for _, test := range tests {
		var actual, _, err = testQueryBuilder.order(test.values)
		if err != nil {
			if test.err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			} else {
				t.Fatalf("unexpected error: %v", err)
			}

			continue
		}

		assert.Equal(t, test.expected, actual, "mismatched orders")
	}
}

func TestOptimizedFilter(t *testing.T) {
	type conditionTestCase struct {
		values map[string][]string

		expected *conditions
		empty    bool
		err      error
	}

	var tests = []conditionTestCase{
		{
			values: map[string][]string{
				"id":         {"1", "2"},
				"type":       {"value"},
				"start_time": {"le(20000)", "ge(10000)"},
			},
			expected: &conditions{
				Filter: selectors{
					{
						selectorTerms: {
							"id": []interface{}{int64(1), int64(2)},
						},
					},
					{
						selectorRange: map[string]interface{}{
							"start_time": map[string]interface{}{
								"lt": time.Unix(20000, 0).UTC().Add(1).Format(timeStoreFormatPrecise),
								"gt": time.Unix(10000, 0).UTC().Add(-1).Format(timeStoreFormatPrecise),
							},
						},
					},
					{
						selectorTerms: {
							"type": []interface{}{"value"},
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"resource_name": {"isnull()"},
			},
			expected: &conditions{
				MustNot: selectors{
					{
						selectorExists: {
							"field": "resource_name",
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"resource_name": {"notnull()"},
			},
			expected: &conditions{
				Filter: selectors{
					{
						selectorExists: {
							"field": "resource_name",
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"policy_name":   {"isnull()"},
				"resource_name": {"isnull()"},
			},
			expected: &conditions{
				MustNot: selectors{
					{
						selectorExists: {
							"field": "policy_name",
						},
					},
					{
						selectorExists: {
							"field": "resource_name",
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"policy_name":   {"notnull()"},
				"resource_name": {"isnull()"},
			},
			expected: &conditions{
				Filter: selectors{
					{
						selectorExists: {
							"field": "policy_name",
						},
					},
				},
				MustNot: selectors{
					{
						selectorExists: {
							"field": "resource_name",
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"type":    {"notnull()"},
				"context": {"isnull()"},
			},
			err: fmt.Errorf("bad condition field 'context'"),
		},
		{
			values: map[string][]string{
				"Non_existent_col": {"1", "2"},
				"type":             {"value"},
			},
			err: fmt.Errorf("bad condition field 'Non_existent_col'"),
		},
		{
			values: map[string][]string{
				"id":   {"1", "2_not_an_id"},
				"type": {"value"},
			},
			err: fmt.Errorf("failed to generate condition for column id: strconv.ParseInt: parsing \"2_not_an_id\": invalid syntax on field 'id'"),
		},
		{
			values: map[string][]string{
				"uuid": {"not_an_uuid"},
			},
			err: fmt.Errorf("failed to generate condition for column uuid: bad uuid format: invalid UUID length: 11"),
		},
		{
			values: map[string][]string{
				"":     {"1", "2"},
				"type": {"value"},
			},
			err: fmt.Errorf("empty condition field"),
		},
	}

	for _, test := range tests {
		var res, empty, err = testQueryBuilder.filter(true, test.values)
		if err != nil {
			if test.err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			} else {
				t.Fatalf("unexpected error: %v", err)
			}

			continue
		}

		assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
		if empty {
			continue
		}

		assert.Equal(t, test.expected, res, "mismatched fields")
	}
}

func TestNonOptimizedFilter(t *testing.T) {
	type conditionTestCase struct {
		values map[string][]string

		expected *conditions
		empty    bool
		err      error
	}

	var tests = []conditionTestCase{
		{
			values: map[string][]string{
				"id":         {"1", "2"},
				"type":       {"value"},
				"start_time": {"le(20000)", "ge(10000)"},
			},
			expected: &conditions{
				Filter: selectors{
					{
						selectorTerms: {
							"id": []interface{}{int64(1), int64(2)},
						},
					},
					{
						selectorRange: map[string]interface{}{
							"start_time": map[string]interface{}{
								"lt": time.Unix(20000, 0).UTC().Add(1).Format(timeStoreFormatPrecise),
							},
						},
					},
					{
						selectorRange: map[string]interface{}{
							"start_time": map[string]interface{}{
								"gt": time.Unix(10000, 0).UTC().Add(-1).Format(timeStoreFormatPrecise),
							},
						},
					},
					{
						selectorTerms: {
							"type": []interface{}{"value"},
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"resource_name": {"isnull()"},
			},
			expected: &conditions{
				MustNot: selectors{
					{
						selectorExists: {
							"field": "resource_name",
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"resource_name": {"notnull()"},
			},
			expected: &conditions{
				Filter: selectors{
					{
						selectorExists: {
							"field": "resource_name",
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"policy_name":   {"isnull()"},
				"resource_name": {"isnull()"},
			},
			expected: &conditions{
				MustNot: selectors{
					{
						selectorExists: {
							"field": "policy_name",
						},
					},
					{
						selectorExists: {
							"field": "resource_name",
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"policy_name":   {"notnull()"},
				"resource_name": {"isnull()"},
			},
			expected: &conditions{
				Filter: selectors{
					{
						selectorExists: {
							"field": "policy_name",
						},
					},
				},
				MustNot: selectors{
					{
						selectorExists: {
							"field": "resource_name",
						},
					},
				},
			},
		},
		{
			values: map[string][]string{
				"type":    {"notnull()"},
				"context": {"isnull()"},
			},
			err: fmt.Errorf("bad condition field 'context'"),
		},
		{
			values: map[string][]string{
				"Non_existent_col": {"1", "2"},
				"type":             {"value"},
			},
			err: fmt.Errorf("bad condition field 'Non_existent_col'"),
		},
		{
			values: map[string][]string{
				"id":   {"1", "2_not_an_id"},
				"type": {"value"},
			},
			err: fmt.Errorf("failed to generate condition for column id: strconv.ParseInt: parsing \"2_not_an_id\": invalid syntax on field 'id'"),
		},
		{
			values: map[string][]string{
				"uuid": {"not_an_uuid"},
			},
			err: fmt.Errorf("failed to generate condition for column uuid: bad uuid format: invalid UUID length: 11"),
		},
		{
			values: map[string][]string{
				"":     {"1", "2"},
				"type": {"value"},
			},
			err: fmt.Errorf("empty condition field"),
		},
	}

	for _, test := range tests {
		var res, empty, err = testQueryBuilder.filter(false, test.values)
		if err != nil {
			if test.err != nil {
				assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for values %v", test.values)
			} else {
				t.Fatalf("unexpected error: %v", err)
			}

			continue
		}

		assert.Equalf(t, test.empty, empty, "failure in test for values %v", test.values)
		if empty {
			continue
		}

		assert.Equal(t, test.expected, res, "mismatched fields")
	}
}

func fieldSel(f selector) selectorType {
	for k := range f {
		return k
	}
	return ""
}

func valuesSel(f selector) string {
	for k := range f[fieldSel(f)] {
		return k
	}
	return ""
}

func valueSel(f selector) interface{} {
	return f[fieldSel(f)][valuesSel(f)]
}

func TestSelectors(t *testing.T) {
	var expected = selector{
		"outer": {
			"inner": 5,
		},
	}
	var actual = newSelector("outer", "inner", 5)

	assert.Equal(t, expected, actual, "named field not generated as expectedf")
	assert.Equal(t, fieldSel(expected), fieldSel(actual), "different outer fields")
	assert.Equal(t, valuesSel(expected), valuesSel(actual), "different inner fields")
	assert.Equal(t, valueSel(expected), valueSel(actual), "different value fields")
}
