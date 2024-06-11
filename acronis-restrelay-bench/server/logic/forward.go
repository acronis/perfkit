package logic

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
)

func configureHTTPClient() http.Client {
	return http.Client{
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 100,
		},
	}
}

var clientHTTP = configureHTTPClient()

func parseOptionalConnectionHeaderInURL(args URIArgs) string {
	values := args.Get("connection")
	if len(values) != 1 {
		return ""
	}
	if values[0] != "keep-alive" && values[0] != "close" {
		return ""
	}

	return values[0]
}

func forward(baseURL string, parentRequest RequestWrapper, correlationID string, reqBody []byte) (body []byte, err error) {
	if baseURL == "" {
		return nil, errors.New("base URL is empty, Forward failed")
	}

	var reqBodyBuffer io.Reader
	if reqBody != nil {
		reqBodyBuffer = bytes.NewBuffer(reqBody)
	}

	req, err := http.NewRequest("GET", baseURL+parentRequest.URI, reqBodyBuffer)
	if err != nil {
		return
	}

	connectionHeader := parseOptionalConnectionHeaderInURL(parentRequest.Args)
	if connectionHeader == "" {
		connectionHeader = parentRequest.ConnectionHeader
	}
	if connectionHeader != "" {
		req.Header.Add("Connection", connectionHeader)
	}

	req.Header.Add("X-Correlation-ID", correlationID)

	resp, err := clientHTTP.Do(req)
	if err != nil {
		return
	}

	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return
	}

	err = resp.Body.Close()
	if err != nil {
		return
	}

	return
}

type forwardDataAction struct {
	size int64
}

func (forwardDataArgs *forwardDataAction) parseParameters(params map[string]string) error {
	var size, ok = params["size"]
	if !ok {
		return errors.New("size parameter is missing")
	}

	var err error
	forwardDataArgs.size, err = parseFileSize(size)
	if err != nil {
		return fmt.Errorf("failed conversion string to int in ForwardDataArguments with: %v", err)
	}

	return nil
}
