package logic

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// TimestampBuilder is a struct for building timestamps
type TimestampBuilder struct {
	name       string
	timestamps []int64
	marshalled []string
}

// NewTimestampBuilder creates a new TimestampBuilder
func NewTimestampBuilder(name string) TimestampBuilder {
	return TimestampBuilder{
		name:       name,
		timestamps: make([]int64, 0),
		marshalled: make([]string, 0),
	}
}

// AddTimestamp adds a timestamp to the TimestampBuilder
func (tb *TimestampBuilder) AddTimestamp() {
	tb.timestamps = append(tb.timestamps, time.Now().UnixNano())
}

// BuildTimestamps builds timestamps
func (tb *TimestampBuilder) BuildTimestamps() (res string) {
	if len(tb.timestamps) == 0 {
		return
	}
	for _, ts := range tb.timestamps {
		res += fmt.Sprintf("%d, ", ts)
	}
	res = fmt.Sprintf("[%s]", res[:len(res)-2])

	return
}

func (tb *TimestampBuilder) buildMarshalled() (res string) {
	if len(tb.marshalled) == 0 {
		return "[]"
	}
	for _, m := range tb.marshalled {
		res += fmt.Sprintf("%s, ", m) //nolint:perfsprint
	}
	res = fmt.Sprintf("[%s]", res[:len(res)-2])

	return
}

// AddMarshalledTimestamps adds marshalled timestamps to the TimestampBuilder
func (tb *TimestampBuilder) AddMarshalledTimestamps(marshalled string) {
	tb.marshalled = append(tb.marshalled, marshalled)
}

// Marshal marshals the TimestampBuilder
func (tb *TimestampBuilder) Marshal() (res string) {
	var timestamps = tb.BuildTimestamps()
	var marshalleds = tb.buildMarshalled()

	res = fmt.Sprintf("{\"name\": \"%s\", \"timestamps\": %s, \"marshalled\": %s}", tb.name, timestamps, marshalleds)

	return
}

// ThreadSafeCounter is a thread-safe counter
type ThreadSafeCounter struct {
	counter int64
}

// NewThreadSafeCounter creates a new ThreadSafeCounter
func NewThreadSafeCounter() ThreadSafeCounter {
	return ThreadSafeCounter{
		counter: 0,
	}
}

// Inc increments the counter
func (counter *ThreadSafeCounter) Inc() (res int64) {
	res = atomic.AddInt64(&counter.counter, 1)

	return
}

func parseFunctionString(input string) (string, map[string]string, error) {
	pattern := `^(\w+)(?:\(([^)]*)\))?$`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(input)
	if len(matches) < 2 {
		return "", nil, errors.New("invalid input format")
	}

	function := matches[1]

	arguments := make(map[string]string)

	// Check if there are arguments
	if len(matches) == 3 && matches[2] != "" {
		argsString := matches[2]
		argsPairs := strings.Split(argsString, ",")
		for _, pair := range argsPairs {
			keyValue := strings.Split(pair, "=")
			if len(keyValue) != 2 {
				return "", nil, errors.New("invalid argument format")
			}
			key := keyValue[0]
			value := keyValue[1]
			arguments[key] = value
		}
	}

	return function, arguments, nil
}

func parseFileSize(sizeStr string) (int64, error) {
	pattern := `^([\d.]+)([KMGTP]?B)$`
	re := regexp.MustCompile(pattern)

	matches := re.FindStringSubmatch(sizeStr)
	if len(matches) != 3 {
		return 0, errors.New("invalid size format")
	}

	valueStr := matches[1]
	unit := matches[2]

	value, err := strconv.ParseFloat(valueStr, 64)
	if err != nil {
		return 0, err //nolint:wrapcheck
	}

	switch strings.ToUpper(unit) {
	case "B":
		return int64(value), nil
	case "KB":
		return int64(value * 1024), nil
	case "MB":
		return int64(value * 1024 * 1024), nil
	case "GB":
		return int64(value * 1024 * 1024 * 1024), nil
	case "TB":
		return int64(value * 1024 * 1024 * 1024 * 1024), nil
	case "PB":
		return int64(value * 1024 * 1024 * 1024 * 1024 * 1024), nil
	default:
		return 0, errors.New("unknown unit")
	}
}
