// nolint: gosimple
package optimize

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestOptimizeIdCond(t *testing.T) {
	type testOptimizeIdCond = struct {
		col    string
		values []string

		min   int64
		max   int64
		list  []int64
		empty bool
		err   error
	}

	var minInt = int64(math.MinInt64)
	var maxInt = int64(math.MaxInt64)

	var tests = []testOptimizeIdCond{
		{
			col:    "id",
			values: []string{},

			empty: true,
		},
		{
			col:    "id",
			values: []string{"123", "456"},

			list: []int64{123, 456},
		},
		{
			col:    "id",
			values: []string{"le(123)"},

			min: minInt,
			max: 124,
		},
		{
			col:    "id",
			values: []string{"gt(123)"},

			min: 123,
			max: maxInt,
		},
		{
			col:    "id",
			values: []string{"gt(123)", "121"},

			empty: true,
		},
		{
			col:    "task_id",
			values: []string{"gt(123)", "124"},

			list: []int64{124},
		},
		{
			col:    "task_id",
			values: []string{"lt(130)", "124"},

			list: []int64{124},
		},
		{
			col:    "task_id",
			values: []string{"gt(123)", "le(129)"},

			min: 123,
			max: 130,
		},
		{
			col:    "task_id",
			values: []string{"gt(123)", "le(129)", "124"},

			list: []int64{124},
		},
		{
			col:    "task_id",
			values: []string{"gt(129)", "le(121)"},

			empty: true,
		},
		{
			col:    "id",
			values: []string{"gt(123)", "le(129)", "124", "126"},

			list: []int64{124, 126},
		},
		{
			col:    "id",
			values: []string{"gt(123)", "le(129)", "121", "130"},

			empty: true,
		},
		{
			col:    "id",
			values: []string{"gt(123)", "le(129)", "121", "124", "130"},

			list: []int64{124},
		},
		{
			col:    "id",
			values: []string{"gt(123)", "lt(129)", "124", "129"},

			list: []int64{124},
		},
		{
			col:    "id",
			values: []string{"gt(123)", "lt(129)", "125", "124"},

			list: []int64{125, 124},
		},
		{
			col:    "id",
			values: []string{"gt(123)", "lt(129)", "129", "124"},

			list: []int64{124},
		},
		{
			col:    "id",
			values: []string{"gt(123)", "lt(129)", "129", "124", "lt(128)"},

			list: []int64{124},
		},
		{
			col:    "id",
			values: []string{"gt(123)", "lt(128)", "lt(129)", "129", "124"},

			list: []int64{124},
		},
		{
			col:    "id",
			values: []string{"gt(123)", "ge(124)", "lt(128)", "lt(129)", "129", "124"},

			list: []int64{124},
		},
		{
			col:    "id",
			values: []string{"gt(123)", "ge(124)", "lt(128)", "lt(129)", "129", "124"},

			list: []int64{124},
		},
		{
			col:    "id",
			values: []string{"ge(125)", "gt(123)", "lt(128)", "lt(129)", "129", "125"},

			list: []int64{125},
		},
		{
			col:    "id",
			values: []string{"ge(125)", "gt(123)", "lt(128)", "lt(129)", "129", "125"},

			list: []int64{125},
		},
		{
			col:    "id",
			values: []string{"130a"},

			err: fmt.Errorf("strconv.ParseInt: parsing \"130a\": invalid syntax on field 'id'"),
		},
		{
			col:    "id",
			values: []string{"hex(130)"},

			err: fmt.Errorf("unsupported function 'hex' on id field"),
		},
		{
			col:    "id",
			values: []string{"hex(130"},

			err: fmt.Errorf("bad function 'hex(130', no closing bracket on field 'id'"),
		},
	}

	for _, test := range tests {
		var min, max, equalityList, empty, err = IDCond(test.col, test.values)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for col %s with values %v", test.col, test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "wrong empty value in test for col %s with values %v", test.col, test.values)
		if empty {
			continue
		}

		assert.Equalf(t, test.list, equalityList, "wrong list value in test for col %s with values %v", test.col, test.values)
		if len(equalityList) != 0 {
			continue
		}

		assert.Equalf(t, test.min, min, "wrong min value in test for col %s with values %v", test.col, test.values)
		assert.Equalf(t, test.max, max, "wrong max value in test for col %s with values %v", test.col, test.values)
	}
}

type testEnumStringAPIType string

const (
	testEnumStringAPIHigh        testEnumStringAPIType = "high"
	testEnumStringAPIAboveNormal testEnumStringAPIType = "aboveNormal"
	testEnumStringAPINormal      testEnumStringAPIType = "normal"
	testEnumStringAPIBelowNormal testEnumStringAPIType = "belowNormal"
	testEnumStringAPILow         testEnumStringAPIType = "low"
)

const (
	testEnumStringDBHigh      int64 = 20
	testEnumStringDBAboveNorm int64 = 10
	testEnumStringDBNorm      int64 = 0
	testEnumStringDBBelowNorm int64 = -10
	testEnumStringDBLow       int64 = -20
	testEnumStringDBCount     int   = 5
)

func enumIntValues() []int64 {
	return []int64{
		testEnumStringDBHigh,
		testEnumStringDBAboveNorm,
		testEnumStringDBNorm,
		testEnumStringDBBelowNorm,
		testEnumStringDBLow,
	}
}

func enumStringToInt(v string) (int64, error) {
	switch testEnumStringAPIType(v) {
	case testEnumStringAPIHigh:
		return testEnumStringDBHigh, nil
	case testEnumStringAPIAboveNormal:
		return testEnumStringDBAboveNorm, nil
	case testEnumStringAPINormal:
		return testEnumStringDBNorm, nil
	case testEnumStringAPIBelowNormal:
		return testEnumStringDBBelowNorm, nil
	case testEnumStringAPILow:
		return testEnumStringDBLow, nil
	default:
		return 0, fmt.Errorf("incorrect test priority value '%v'", v)
	}
}

func enumIntToString(i int64) testEnumStringAPIType {
	switch i {
	case testEnumStringDBHigh:
		return testEnumStringAPIHigh
	case testEnumStringDBAboveNorm:
		return testEnumStringAPIAboveNormal
	case testEnumStringDBNorm:
		return testEnumStringAPINormal
	case testEnumStringDBBelowNorm:
		return testEnumStringAPIBelowNormal
	case testEnumStringDBLow:
		return testEnumStringAPILow
	default:
		return testEnumStringAPINormal
	}
}

func TestOptimizeEnumStringCond(t *testing.T) {
	type testOptimizeEnumStringCond = struct {
		col        string
		values     []string
		conv       func(string) (int64, error)
		enumSize   int
		enumValues []int64

		list  []int64
		empty bool
		err   error
	}

	var tests = []testOptimizeEnumStringCond{
		{
			col:        "priority",
			values:     []string{},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			empty: true,
		},
		{
			col:        "priority",
			values:     []string{string(testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{testEnumStringDBNorm},
		},
		{
			col:        "priority",
			values:     []string{string(testEnumStringAPINormal), string(testEnumStringAPIHigh)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{testEnumStringDBNorm, testEnumStringDBHigh},
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("lt(%s)", testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{testEnumStringDBBelowNorm, testEnumStringDBLow},
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("le(%s)", testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{testEnumStringDBNorm, testEnumStringDBBelowNorm, testEnumStringDBLow},
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("ge(%s)", testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{testEnumStringDBHigh, testEnumStringDBAboveNorm, testEnumStringDBNorm},
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("gt(%s)", testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{testEnumStringDBHigh, testEnumStringDBAboveNorm},
		},
		{
			col:        "priority",
			values:     []string{string(testEnumStringAPINormal), string(testEnumStringAPILow), string(testEnumStringAPIBelowNormal), string(testEnumStringAPIAboveNormal), string(testEnumStringAPIHigh)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{},
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("gt(%s)", testEnumStringAPINormal), fmt.Sprintf("le(%s)", testEnumStringAPIBelowNormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			empty: true,
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("gt(%s)", testEnumStringAPIBelowNormal), fmt.Sprintf("le(%s)", testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{testEnumStringDBNorm},
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("le(%s)", testEnumStringAPIHigh), fmt.Sprintf("ge(%s)", testEnumStringAPILow), string(testEnumStringAPINormal), string(testEnumStringAPILow), string(testEnumStringAPIBelowNormal), string(testEnumStringAPIAboveNormal), string(testEnumStringAPIHigh)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			list: []int64{},
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("le(%s)", testEnumStringAPIAboveNormal), fmt.Sprintf("ge(%s)", testEnumStringAPILow), string(testEnumStringAPIHigh)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			empty: true,
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("very_%s", testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			err: fmt.Errorf("incorrect test priority value 'very_normal' on field 'priority'"),
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("hex(%s", testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			err: fmt.Errorf("bad function 'hex(normal', no closing bracket on field 'priority'"),
		},
		{
			col:        "priority",
			values:     []string{fmt.Sprintf("hex(%s)", testEnumStringAPINormal)},
			conv:       enumStringToInt,
			enumSize:   testEnumStringDBCount,
			enumValues: enumIntValues(),

			err: fmt.Errorf("unsupported function 'hex' on priority field"),
		},
	}

	for _, test := range tests {
		var equalityList, empty, err = EnumStringCond(test.col, test.values, test.conv, test.enumSize, test.enumValues)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for col %s with values %v", test.col, test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "wrong empty value in test for col %s with values %v", test.col, test.values)
		if empty {
			continue
		}

		assert.Equalf(t, test.list, equalityList, "wrong list value in test for col %s with values %v", test.col, test.values)
	}
}

func TestOptimizeTimeCond(t *testing.T) {
	type testOptimizeTimeCond = struct {
		col    string
		values []string

		min   time.Time
		max   time.Time
		list  []time.Time
		empty bool
		err   error
	}

	var today = time.Date(2023, 3, 28, 17, 27, 40, 0, time.UTC)

	var tests = []testOptimizeTimeCond{
		{
			col:    "creation_time_ns",
			values: []string{},

			empty: true,
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("lt(%s)", today.Format(time.RFC3339))},

			min: minTime,
			max: today,
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("le(%s)", today.Format(time.RFC3339))},

			min: minTime,
			max: today.Add(1),
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("gt(%s)", today.Format(time.RFC3339))},

			min: today,
			max: maxTime,
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("ge(%s)", today.Format(time.RFC3339))},

			min: today.Add(-1),
			max: maxTime,
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("lt(%s)", today.Format(time.RFC3339)), fmt.Sprintf("gt(%s)", today.Add(time.Minute).Format(time.RFC3339))},

			empty: true,
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("%s", today.Format(time.RFC3339))},

			list: []time.Time{today},
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("%s", today.Format(time.RFC3339)), fmt.Sprintf("lt(%s)", today.Add(time.Minute).Format(time.RFC3339)), fmt.Sprintf("%s", today.Add(2*time.Minute).Format(time.RFC3339))},

			list: []time.Time{today},
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("gt(%s)", today.Add(-time.Minute).Format(time.RFC3339)), fmt.Sprintf("%s", today.Format(time.RFC3339)), fmt.Sprintf("lt(%s)", today.Add(time.Minute).Format(time.RFC3339)), fmt.Sprintf("%s", today.Add(2*time.Minute).Format(time.RFC3339))},

			list: []time.Time{today},
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("gt(%s)", today.Add(-time.Minute).Format(time.RFC3339)), fmt.Sprintf("%s", today.Format(time.RFC3339)), fmt.Sprintf("%s", today.Add(-2*time.Minute).Format(time.RFC3339))},

			list: []time.Time{today},
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("gt(%s)", today.Format(time.RFC3339)), fmt.Sprintf("gt(%s)", today.Add(-time.Minute).Format(time.RFC3339))},

			min: today,
			max: maxTime,
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("lt(%s)", today.Format(time.RFC3339)), fmt.Sprintf("lt(%s)", today.Add(time.Minute).Format(time.RFC3339))},

			min: minTime,
			max: today,
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("gt(%s)", today.Add(-time.Minute).Format(time.RFC3339)), fmt.Sprintf("lt(%s)", today.Add(time.Minute).Format(time.RFC3339)), fmt.Sprintf("%s", today.Add(2*time.Minute).Format(time.RFC3339))},

			empty: true,
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("%s1", today.Format(time.RFC3339))},

			err: fmt.Errorf("incorrect time format, must be one of (UNIX-TIMESTAMP-NANO, UNIX-TIMESTAMP, RFC3339, RFC1123, RFC850, ANSI-C) on field 'creation_time_ns'"),
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("hex(%s)", today.Format(time.RFC3339))},

			err: fmt.Errorf("unsupported function 'hex' on field 'creation_time_ns'"),
		},
		{
			col:    "creation_time_ns",
			values: []string{fmt.Sprintf("hex(%s", today.Format(time.RFC3339))},

			err: fmt.Errorf("bad function 'hex(2023-03-28T17:27:40Z', no closing bracket on field 'creation_time_ns'"),
		},
	}

	for _, test := range tests {
		var min, max, equalityList, empty, err = TimeCond(test.col, test.values)
		if err != nil {
			assert.Equalf(t, test.err.Error(), err.Error(), "failure in test for col %s with values %v", test.col, test.values)
			continue
		}

		assert.Equalf(t, test.empty, empty, "wrong empty value in test for col %s with values %v", test.col, test.values)
		if empty {
			continue
		}

		assert.Equalf(t, test.list, equalityList, "wrong list value in test for col %s with values %v", test.col, test.values)
		if len(equalityList) != 0 {
			continue
		}

		assert.Equalf(t, test.min, min, "wrong min value in test for col %s with values %v", test.col, test.values)
		assert.Equalf(t, test.max, max, "wrong max value in test for col %s with values %v", test.col, test.values)
	}
}
