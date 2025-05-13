package sql

import (
	"fmt"
	"reflect"
	"time"

	"github.com/stretchr/testify/assert"
)

func (suite *TestingSuite) TestStringNeCond() {
	var actualDialect, err = dbDialect(suite.ConnString)
	if err != nil {
		suite.T().Errorf("failed to get actual db dialect, err: %v", err)
		return
	}

	var condition = stringCond(64, false)

	type stringNeCondTest struct {
		values        []string
		qry           []string
		args          []interface{}
		expectedError string
	}

	var stringNeCondTests = []stringNeCondTest{
		{
			values: []string{"gt(v1)"},
			qry:    []string{"type > %v"},
			args:   []interface{}{"v1"},
		},
		{
			values: []string{"gt(v1)", "lt(v2)"},
			qry:    []string{"type > %v", "type < %v"},
			args:   []interface{}{"v1", "v2"},
		},
		{
			values: []string{"v1"},
			qry:    []string{"type = %v"},
			args:   []interface{}{"v1"},
		},
		{
			values: []string{"ne(v2)"},
			qry:    []string{"type <> %v"},
			args:   []interface{}{"v2"},
		},
		{
			values: []string{"v1", "v2"},
			qry:    []string{"type IN (%v)"},
			args:   []interface{}{[]string{"v1", "v2"}},
		},
		{
			values: []string{"ne(v1)", "ne(v2)"},
			qry:    []string{"type NOT IN (%v)"},
			args:   []interface{}{[]string{"v1", "v2"}},
		},
		{
			values: []string{"v1", "ne(v2)"},
			qry:    []string{"type = %v"},
			args:   []interface{}{"v1"},
		},
		{
			values: []string{"ne(v1)", "v2"},
			qry:    []string{"type = %v"},
			args:   []interface{}{"v2"},
		},
		{
			values: []string{"ne(v1)", "v2"},
			qry:    []string{"type = %v"},
			args:   []interface{}{"v2"},
		},
		{
			values: []string{"v1", "ne(v2)", "ne(v3)"},
			qry:    []string{"type = %v"},
			args:   []interface{}{"v1"},
		},
		{
			values:        []string{"v1", "ne(v1)"},
			expectedError: "positive condition on value cannot be set alongside with ne function on it, field 'type', value 'v1'",
		},
	}

	for _, test := range stringNeCondTests {
		for _, optimzed := range []bool{true, false} {
			if qry, args, err := condition(actualDialect, optimzed, "type", test.values); err != nil {
				if test.expectedError == "" {
					suite.T().Errorf("failed create query condition for values %v, err: %v", test.values, err)
				} else if err.Error() != test.expectedError {
					suite.T().Errorf("wrong expected error for creating query condition for values %v, got err: %v, expected: %v", test.values, err, test.expectedError)
				}
			} else {
				if !reflect.DeepEqual(qry, test.qry) {
					suite.T().Errorf("query condition for values %v is wrong, got %s, expected: %s", test.values, qry, test.qry)
				}
				if !reflect.DeepEqual(args, test.args) {
					suite.T().Errorf("query condition for values %v is wrong, got %v, expected: %v", test.values, args, test.args)
				}
			}
		}
	}
}

func (suite *TestingSuite) TestConvFnc() {
	tests := []struct {
		fnc      string
		field    string
		expected string
		err      error
	}{
		{"", "age", "age = %v", nil},
		{"lt", "age", "age < %v", nil},
		{"le", "age", "age <= %v", nil},
		{"gt", "age", "age > %v", nil},
		{"ge", "age", "age >= %v", nil},
		{"unknown", "age", "", fmt.Errorf("unsupported function")},
	}

	for _, test := range tests {
		actual, err := convFnc(test.fnc, test.field)
		if err != nil {
			assert.Equal(suite.T(), test.err, err, "unexpected error for fnc: %v, field: %v", test.fnc, test.field)
		} else {
			assert.NoError(suite.T(), err)
			assert.Equal(suite.T(), test.expected, actual, "unexpected result for fnc: %v, field: %v", test.fnc, test.field)
		}
	}
}

func (suite *TestingSuite) TestIdCond() {
	tests := []struct {
		field    string
		values   []string
		expected []string
		vals     []interface{}
		err      error
	}{
		{"id", []string{"1"}, []string{"id = %v"}, []interface{}{int64(1)}, nil},
		{"id", []string{"1", "2"}, []string{"id IN (%v)"}, []interface{}{[]int64{1, 2}}, nil},
		{"id", []string{"lt(5)"}, []string{"id < %v"}, []interface{}{int64(5)}, nil},
		{"id", []string{"not_a_number"}, nil, nil, fmt.Errorf("strconv.ParseInt: parsing \"not_a_number\": invalid syntax on field 'id'")},
	}

	for _, test := range tests {
		for _, optimzed := range []bool{true, false} {
			actualConds, actualVals, err := idCond()(nil, optimzed, test.field, test.values)
			if err != nil {
				assert.Equal(suite.T(), test.err.Error(), err.Error(), "unexpected error for values: %v", test.values)
			} else {
				assert.NoError(suite.T(), err)
				assert.Equal(suite.T(), test.expected, actualConds, "unexpected conditions for values: %v", test.values)
				assert.Equal(suite.T(), test.vals, actualVals, "unexpected values for values: %v", test.values)
			}
		}
	}
}

func (suite *TestingSuite) TestUuidCond() {
	tests := []struct {
		field    string
		values   []string
		expected []string
		vals     []interface{}
		err      error
	}{
		{"uuid", []string{"00000000-0000-0000-0000-000000000001"}, []string{"uuid = %v"}, []interface{}{"00000000-0000-0000-0000-000000000001"}, nil},
		{"uuid", []string{"gt(00000000-0000-0000-0000-000000000001)"}, []string{"uuid > %v"}, []interface{}{"00000000-0000-0000-0000-000000000001"}, nil},
		{"uuid", []string{"invalid_uuid"}, nil, nil, fmt.Errorf("bad uuid format: invalid UUID length: 12 on field 'uuid'")},
	}

	for _, test := range tests {
		for _, optimzed := range []bool{true, false} {
			actualConds, actualVals, err := uuidCond()(nil, optimzed, test.field, test.values)
			if err != nil {
				assert.Equal(suite.T(), test.err.Error(), err.Error(), "unexpected error for values: %v", test.values)
			} else {
				assert.NoError(suite.T(), err)
				assert.Equal(suite.T(), test.expected, actualConds, "unexpected conditions for values: %v", test.values)
				assert.Equal(suite.T(), test.vals, actualVals, "unexpected values for values: %v", test.values)
			}
		}
	}
}

func (suite *TestingSuite) TestStringCond() {
	tests := []struct {
		field       string
		values      []string
		maxValueLen int
		allowLikes  bool
		expected    []string
		vals        []interface{}
		err         error
	}{
		{"name", []string{"val1"}, 16, false, []string{"name = %v"}, []interface{}{"val1"}, nil},
		{"name", []string{"hlike(val1)"}, 16, true, []string{"name LIKE %v"}, []interface{}{"val1%%"}, nil},
		{"name", []string{"val1", "val2"}, 16, false, []string{"name IN (%v)"}, []interface{}{[]string{"val1", "val2"}}, nil},
		{"name", []string{"like(val1)"}, 16, false, nil, nil, fmt.Errorf("like functions are unsupported on field 'name'")},
	}

	for _, test := range tests {
		for _, optimized := range []bool{true, false} {
			actualConds, actualVals, err := stringCond(test.maxValueLen, test.allowLikes)(nil, optimized, test.field, test.values)
			if err != nil {
				assert.Equal(suite.T(), test.err.Error(), err.Error(), "unexpected error for values: %v", test.values)
			} else {
				assert.NoError(suite.T(), err)
				assert.Equal(suite.T(), test.expected, actualConds, "unexpected conditions for values: %v", test.values)
				assert.Equal(suite.T(), test.vals, actualVals, "unexpected values for values: %v", test.values)
			}
		}
	}
}

func (suite *TestingSuite) TestEnumStringCond() {
	type enumStringCondTestCase struct {
		values   []string
		expected []string
		vals     []interface{}
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

	// Conversion function for enum values
	var convFunc = func(s string) (int64, error) {
		if v, ok := enumConversionMap[s]; ok {
			return v, nil
		}
		return 0, fmt.Errorf("invalid enum value: `%s`", s)
	}

	var tests = []enumStringCondTestCase{
		{
			values:   []string{"val1"},
			expected: []string{"state = %v"},
			vals:     []interface{}{int64(1)},
		},
		{
			values:   []string{"val1", "val2"},
			expected: []string{"state IN (%v)"},
			vals:     []interface{}{[]int64{1, 2}},
		},
		{
			values:   []string{"val1", "val2", "val1"},
			expected: []string{"state IN (%v)"},
			vals:     []interface{}{[]int64{1, 2}},
		},
		/*
			{
				values: []string{"val1", "val2", "val3", "val4"},
				empty:  true,
			},

		*/
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
		for _, optimized := range []bool{true, false} {
			actualConds, actualVals, err := enumStringCond(convFunc, defaultMaxValues, enumValues)(nil, optimized, defaultColumn, test.values)
			if err != nil {
				if test.err != nil {
					assert.Equalf(suite.T(), test.err.Error(), err.Error(), "failure in test for values %v", test.values)
				} else {
					suite.T().Fatalf("unexpected error: %v", err)
				}

				continue
			}

			assert.Equalf(suite.T(), test.empty, len(actualConds) == 0, "unexpected empty state for values %v", test.values)
			if len(actualConds) == 0 {
				continue
			}

			assert.Equalf(suite.T(), test.expected, actualConds, "unexpected conditions for values %v", test.values)
			assert.Equalf(suite.T(), test.vals, actualVals, "unexpected values for values %v", test.values)
		}
	}
}

func (suite *TestingSuite) TestTimeCond() {
	tests := []struct {
		field    string
		values   []string
		expected []string
		vals     []interface{}
		err      error
	}{
		{"enqueue_time", []string{"gt(10000)"}, []string{"enqueue_time > %v"}, []interface{}{time.Unix(10000, 0).UTC().UnixNano()}, nil},
		{"enqueue_time", []string{"lt(20000)"}, []string{"enqueue_time < %v"}, []interface{}{time.Unix(20000, 0).UTC().UnixNano()}, nil},
		{"enqueue_time", []string{"fake_func(10000)"}, nil, nil, fmt.Errorf("unsupported function 'fake_func' on field 'enqueue_time'")},
	}

	for _, test := range tests {
		for _, optimized := range []bool{true, false} {
			actualConds, actualVals, err := timeCond()(nil, optimized, test.field, test.values)
			if err != nil {
				assert.Equal(suite.T(), test.err.Error(), err.Error(), "unexpected error for values: %v", test.values)
			} else {
				assert.NoError(suite.T(), err)
				assert.Equal(suite.T(), test.expected, actualConds, "unexpected conditions for values: %v", test.values)
				assert.Equal(suite.T(), test.vals, actualVals, "unexpected values for values: %v", test.values)
			}
		}
	}
}
