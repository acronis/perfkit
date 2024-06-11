// Package logic implements the business logic of the benchmark server.
package logic

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// URIArgs is an interface for getting values from URI.
type URIArgs interface {
	Get(key string) []string
}

// RequestWrapper is a struct for wrapping request.
type RequestWrapper struct {
	Args             URIArgs
	URI              string
	ConnectionHeader string
}

type action interface {
	perform() error
	parseParameters(params map[string]string) error
}

// PerformActions is a function for processing incoming requests.
func PerformActions(baseURL string, request RequestWrapper,
	nodeName string, tb *TimestampBuilder,
	correlationID string) (reader *bufio.Reader, headers map[string]string, status int, err error) {
	values := request.Args
	status = 200
	headers = make(map[string]string)

	actionsString := values.Get(nodeName)

	for _, actionString := range actionsString {
		if actionString == "" {
			err = fmt.Errorf("PerformActions: got %s with missing action", nodeName)

			return
		}
		var function string
		var arguments map[string]string
		if function, arguments, err = parseFunctionString(actionString); err != nil {
			return
		}

		var act action

		switch function {
		case "sleep":
			act = &sleepAction{}
		case "busyloop":
			act = &busyLoopAction{}
		case "allocate":
			act = &allocationAction{}
		case "db":
			act = &databaseAction{}
		case "serial":
			act = &serializationAction{}
		case "parse":
			act = &parseAction{}
		case "log":
			act = &loggingAction{}
		case "exception":
			panic("Action exception called")
		case "shutdown":
			os.Exit(-1)
		case "status":
			status, err = strconv.Atoi(arguments["status"])
			if err != nil {
				err = fmt.Errorf("PerformActions: error in conversion status string to int with: %v", err)

				return
			}

			continue
		case "contentlength", "chunked":
			// Instead of creating action we do all work directly here, because
			// in other way must reorganize Action model.
			var rawLength, ok = arguments["length"]
			if !ok {
				err = errors.New("length parameter is missing")

				return
			}

			length, err := strconv.Atoi(rawLength)
			if err != nil {
				err = fmt.Errorf("PerformActions: error in conversion length string to int with: %v", err)

				return nil, nil, 200, err
			}

			garbageDataReader := bytes.NewReader(make([]byte, length))
			reader = bufio.NewReader(garbageDataReader)

			if function == "contentlength" {
				headers["Content-Length"] = strconv.Itoa(length)
			}

			return reader, headers, 200, nil
		case "forward", "forwardjson", "forwarddata":
			var reqBody []byte

			if function == "forwardjson" || function == "forwarddata" {
				var reqBodyProvider = &parseAction{}

				if function == "forwardjson" {
					err = reqBodyProvider.parseParameters(arguments)
					if err != nil {
						return
					}
				} else if function == "forwarddata" {
					var forwardData = &forwardDataAction{}
					err = forwardData.parseParameters(arguments)
					if err != nil {
						return
					}

					reqBodyProvider.TreeDepth, reqBodyProvider.TreeWidth = getClosestDepthAndWidth(int(forwardData.size))
				}

				if err = reqBodyProvider.Validate(); err != nil {
					return
				}

				if reqBody, err = cache.GetSerialized(serializationAction(*reqBodyProvider)); err != nil {
					return
				}
			}

			tb.AddTimestamp()
			body, err := forward(baseURL, request, correlationID, reqBody)
			if err != nil {
				err = fmt.Errorf("PerformActions: error in forward -- %v", err)

				return nil, nil, 200, err
			}
			tb.AddTimestamp()
			tb.AddMarshalledTimestamps(string(body))

			continue
		default:
			err = fmt.Errorf("PerformActions: got wrong action %s", function)

			return
		}
		if err != nil {
			err = fmt.Errorf("PerformActions: error in UnmarshalParameters -- %v", err)

			return
		}
		err = act.parseParameters(arguments)
		if err != nil {
			return
		}
		tb.AddTimestamp()
		err = act.perform()
		if err != nil {
			err = fmt.Errorf("PerformActions: error in Perform() -- %v. Action -- %v", err, act)

			return
		}
		tb.AddTimestamp()
	}

	timestamps := strings.NewReader(tb.Marshal())
	reader = bufio.NewReader(timestamps)

	return
}
