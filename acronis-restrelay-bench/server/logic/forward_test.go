package logic

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

type mockURIArgs map[string][]string

func (u mockURIArgs) Get(key string) []string {
	return u[key]
}

func TestForward(t *testing.T) {
	tests := []struct {
		name           string
		baseURL        string
		parentRequest  RequestWrapper
		correlationID  string
		reqBody        []byte
		mockResponse   *http.Response
		expectedBody   []byte
		expectedErr    error
		mockServerFunc func(w http.ResponseWriter, r *http.Request)
	}{
		{
			name:          "Valid request with keep-alive header",
			baseURL:       "http://example.com",
			parentRequest: RequestWrapper{URI: "/test", Args: &mockURIArgs{"connection": {"keep-alive"}}},
			correlationID: "12345",
			reqBody:       []byte("test body"),
			expectedBody:  []byte("response body"),
			mockServerFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, "keep-alive", r.Header.Get("Connection"))
				assert.Equal(t, "12345", r.Header.Get("X-Correlation-ID"))
				w.Write([]byte("response body")) //nolint:errcheck
			},
		},
		{
			name:          "Empty base URL",
			baseURL:       "",
			parentRequest: RequestWrapper{URI: "/test", Args: &mockURIArgs{"connection": {"keep-alive"}}},
			correlationID: "12345",
			reqBody:       []byte("test body"),
			expectedBody:  nil,
			expectedErr:   errors.New("base URL is empty, Forward failed"),
		},
		{
			name:          "Invalid connection header",
			baseURL:       "http://example.com",
			parentRequest: RequestWrapper{URI: "/test", Args: &mockURIArgs{"connection": {"invalid"}}},
			correlationID: "12345",
			reqBody:       []byte("test body"),
			expectedBody:  []byte("response body"),
			mockServerFunc: func(w http.ResponseWriter, r *http.Request) {
				assert.NotEqual(t, "invalid", r.Header.Get("Connection"))
				w.Write([]byte("response body")) //nolint:errcheck
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock server
			mockServer := httptest.NewServer(http.HandlerFunc(tt.mockServerFunc))
			defer mockServer.Close()

			// Update baseURL to the mock server URL
			if tt.baseURL != "" {
				tt.baseURL = mockServer.URL
			}

			// Call the forward function
			body, err := forward(tt.baseURL, tt.parentRequest, tt.correlationID, tt.reqBody)

			// Verify the results
			if tt.expectedErr != nil {
				assert.Error(t, err)
				if err != nil {
					assert.Equal(t, tt.expectedErr.Error(), err.Error())
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedBody, body)
			}
		})
	}
}

func TestForwardDataAction_ParseParameters(t *testing.T) {
	tests := []struct {
		params      map[string]string
		expected    forwardDataAction
		expectError bool
		errMessage  string
	}{
		// Valid cases
		{
			params: map[string]string{
				"size": "1KB",
			},
			expected: forwardDataAction{
				size: 1024,
			},
			expectError: false,
		},
		{
			params: map[string]string{
				"size": "10MB",
			},
			expected: forwardDataAction{
				size: 10 * 1024 * 1024,
			},
			expectError: false,
		},
		// Invalid cases
		{
			params:      map[string]string{},
			expectError: true,
			errMessage:  "size parameter is missing",
		},
		{
			params: map[string]string{
				"size": "invalid",
			},
			expectError: true,
			errMessage:  "failed conversion string to int in ForwardDataArguments with: invalid size format",
		},
		{
			params: map[string]string{
				"size": "unsupported",
			},
			expectError: true,
			errMessage:  "failed conversion string to int in ForwardDataArguments with: invalid size format",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("params=%v", tt.params), func(t *testing.T) {
			action := &forwardDataAction{}
			err := action.parseParameters(tt.params)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errMessage, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected.size, action.size)
			}
		})
	}
}
