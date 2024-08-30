package optimize

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/acronis/perfkit/db"
)

const (
	OpLE = "le"
	OpLT = "lt"
	OpGE = "ge"
	OpGT = "gt"
)

var minTime = time.Unix(-2208988800, 0) // Jan 1, 1900
var maxTime = time.Unix(1<<63-62135596801, 999999999)

func str2id(s string) (int64, error) {
	return strconv.ParseInt(s, 10, 64)
}

func int64Min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func int64Max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// parseIDCondValues - parser of id condition values.
// returns min, max, equalityRange, error
// default values means no condition:
// min = int64(math.MinInt64)
// max = int64(math.MaxInt64)
func parseIDCondValues(field string, values []string) (min int64, max int64, equalityRange []int64, err error) {
	min = int64(math.MinInt64)
	max = int64(math.MaxInt64)

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

	equalityRange = make([]int64, 0, len(values))
	for _, v := range values {
		fnc, id, parseErr := procVal(v)
		if parseErr != nil {
			return 0, 0, nil, parseErr
		}

		switch fnc {
		case "":
			equalityRange = append(equalityRange, id)
		case OpLT:
			max = int64Min(max, id)
		case OpLE:
			max = int64Min(max, id+1)
		case OpGT:
			min = int64Max(min, id)
		case OpGE:
			min = int64Max(min, id-1)
		default:
			return 0, 0, nil, fmt.Errorf("unsupported function '%v' on %s field", fnc, field)
		}
	}

	return min, max, equalityRange, nil
}

// IDCond - optimistic id conditions optimizer.
//
// The fastest execution is when client specifies list of ids
// If some condition (lt, le, gt, ge) occurs, execution will be slowed down
//
// Function check semantic consistency of condition.
// If final list is empty, but initial list of condition was not empty, function return true bool.
func IDCond(field string, values []string) (min int64, max int64, equalityRange []int64, empty bool, err error) {
	if len(values) == 0 {
		return 0, 0, nil, true, nil
	}

	min, max, equalityRange, err = parseIDCondValues(field, values)
	if err != nil {
		return 0, 0, nil, false, err
	}

	var minInt = int64(math.MinInt64)
	var maxInt = int64(math.MaxInt64)

	if min == minInt && max == maxInt {
		// It means that all values were id=%d conditions, so we can return initial list even if it was empty
		return 0, 0, equalityRange, false, nil
	}

	if len(equalityRange) == 0 {
		// It means that there were some condition function, but there is no any range for id values
		if min > max {
			return 0, 0, nil, true, nil
		}
		return min, max, nil, false, nil
	}

	// It means that there were some condition function and equalityRange, so we have to check semantic consistency
	var preciseEqualityRange []int64
	for _, val := range equalityRange {
		switch {
		case min != minInt && max != maxInt:
			if val > min && val < max {
				preciseEqualityRange = append(preciseEqualityRange, val)
			}
		case min != minInt:
			if val > min {
				preciseEqualityRange = append(preciseEqualityRange, val)
			}
		case max != maxInt:
			if val < max {
				preciseEqualityRange = append(preciseEqualityRange, val)
			}
		}
	}

	if len(preciseEqualityRange) == 0 {
		// It means that after optimization the whole condition became meaningless because its intersection set is empty
		return 0, 0, nil, true, nil
	}

	// It means that after optimization the condition is meaningful
	return 0, 0, preciseEqualityRange, false, nil
}

// EnumStringCond - optimistic enum conditions optimizer.
//
// The fastest execution is when client specifies list of elems in enum
// If some condition (lt, le, gt, ge) occurs, execution will be slowed down
//
// Function check semantic consistency of condition.
// If final list is empty, but initial list of condition was not empty, function return true bool.
func EnumStringCond(field string, values []string, conv func(string) (int64, error), enumSize int, enumValues []int64) (preciseEqualityRange []int64, empty bool, err error) {
	if len(values) == 0 {
		return nil, true, nil
	}

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

	var dvalues = db.DedupStrings(values)
	var minInt = int64(math.MinInt64)
	var maxInt = int64(math.MaxInt64)

	var minVal = minInt
	var maxVal = maxInt

	var equalInRange []int64
	for _, v := range dvalues {
		fnc, val, parseErr := procVal(v)
		if parseErr != nil {
			return nil, false, parseErr
		}

		switch fnc {
		case "":
			equalInRange = append(equalInRange, val)
		case OpLT:
			maxVal = int64Min(maxVal, val)
		case OpLE:
			maxVal = int64Min(maxVal, val+1)
		case OpGT:
			minVal = int64Max(minVal, val)
		case OpGE:
			minVal = int64Max(minVal, val-1)
		default:
			return nil, false, fmt.Errorf("unsupported function '%v' on %s field", fnc, field)
		}
	}

	if minVal == minInt && maxVal == maxInt {
		if len(equalInRange) == enumSize {
			return []int64{}, false, nil
		}
		return equalInRange, false, nil
	}

	if len(equalInRange) == 0 && minVal > maxVal {
		return nil, true, nil
	}

	// It means that there were some condition function and equalityRange, so we have to check semantic consistency
	if len(equalInRange) == 0 {
		equalInRange = enumValues
	}

	preciseEqualityRange = make([]int64, 0, len(equalInRange))
	for _, val := range equalInRange {
		switch {
		case minVal != minInt && maxVal != maxInt:
			if val > minVal && val < maxVal {
				preciseEqualityRange = append(preciseEqualityRange, val)
			}
		case minVal != minInt:
			if val > minVal {
				preciseEqualityRange = append(preciseEqualityRange, val)
			}
		case maxVal != maxInt:
			if val < maxVal {
				preciseEqualityRange = append(preciseEqualityRange, val)
			}
		}
	}

	if len(preciseEqualityRange) == enumSize {
		return []int64{}, false, nil
	} else if len(preciseEqualityRange) == 0 {
		return nil, true, nil
	}

	return preciseEqualityRange, false, nil
}

func timeMin(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func timeMax(a, b time.Time) time.Time {
	if a.Before(b) {
		return b
	}
	return a
}

// parseTimeCondValues - parser of time condition values.
// default values means no condition:
// min = time.Unix(-2208988800, 0)  Jan 1, 1900
// max = time.Unix(1<<63-62135596801, 999999999)
func parseTimeCondValues(col string, values []string) (min time.Time, max time.Time, equalityRange []time.Time, err error) {
	min = minTime
	max = maxTime

	var procVal = func(v string) (string, time.Time, error) {
		var t time.Time
		var fnc, val, parseErr = db.ParseFunc(v)
		if parseErr != nil {
			return "", time.Time{}, fmt.Errorf("%v on field '%v'", parseErr, col)
		}

		t, parseErr = db.ParseTimeUTC(val)
		if parseErr != nil {
			return "", time.Time{}, fmt.Errorf("%v on field '%v'", parseErr, col)
		}

		return fnc, t, nil
	}

	equalityRange = make([]time.Time, 0, len(values))
	for _, v := range values {
		var fnc, val, parseErr = procVal(v)
		if parseErr != nil {
			return time.Time{}, time.Time{}, nil, parseErr
		}

		switch fnc {
		case "":
			equalityRange = append(equalityRange, val)
		case OpLT:
			max = timeMin(max, val)
		case OpLE:
			max = timeMin(max, val.Add(1))
		case OpGT:
			min = timeMax(min, val)
		case OpGE:
			min = timeMax(min, val.Add(-1))
		default:
			return time.Time{}, time.Time{}, nil, fmt.Errorf("unsupported function '%v' on field '%v'", fnc, col)
		}
	}

	return min, max, equalityRange, nil
}

func TimeCond(col string, values []string) (min time.Time, max time.Time, equalityRange []time.Time, empty bool, err error) {
	if len(values) == 0 {
		return time.Time{}, time.Time{}, nil, true, nil
	}

	min, max, equalityRange, err = parseTimeCondValues(col, values)
	if err != nil {
		return time.Time{}, time.Time{}, nil, false, err
	}

	if len(equalityRange) == 0 {
		// It means that there were some condition function, but there is no any range for id values
		if min.After(max) {
			return time.Time{}, time.Time{}, nil, true, nil
		}
		return min, max, nil, false, nil
	}

	if min == minTime && max == maxTime {
		// It means that all values were time=%d conditions, so we can return initial list even if it was empty
		return min, max, equalityRange, false, nil
	}

	// It means that there were some condition function and equalityRange, so we have to check semantic consistency
	var equalInRange []time.Time
	for _, val := range equalityRange {
		switch {
		case min != minTime && max != maxTime:
			if val.After(min) && val.Before(max) {
				equalInRange = append(equalInRange, val)
			}
		case min != minTime:
			if val.After(min) {
				equalInRange = append(equalInRange, val)
			}
		case max != maxTime:
			if val.Before(max) {
				equalInRange = append(equalInRange, val)
			}
		}
	}

	if len(equalInRange) == 0 {
		// It means that after optimization the whole condition became meaningless because its intersection set is empty
		return minTime, maxTime, equalInRange, true, nil
	}

	// It means that after optimization the condition is meaningful
	return minTime, maxTime, equalInRange, false, nil
}
