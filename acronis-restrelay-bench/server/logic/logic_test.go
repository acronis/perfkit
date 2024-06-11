package logic

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPerformActions(t *testing.T) {
	tests := []struct {
		name            string
		baseURL         string
		request         RequestWrapper
		nodeName        string
		correlationID   string
		expectedHeaders map[string]string
		expectedStatus  int
		expectedErr     error
	}{
		{
			name:          "Missing action",
			baseURL:       "http://example.com",
			request:       RequestWrapper{Args: mockURIArgs{"testNode": {""}}},
			nodeName:      "testNode",
			correlationID: "12345",
			expectedErr:   errors.New("PerformActions: got testNode with missing action"),
		},
		{
			name:            "Sleep action",
			baseURL:         "http://example.com",
			request:         RequestWrapper{Args: mockURIArgs{"testNode": {"sleep(duration=10ms)"}}},
			nodeName:        "testNode",
			correlationID:   "12345",
			expectedHeaders: map[string]string{},
			expectedStatus:  200,
		},
		{
			name:          "Invalid action",
			baseURL:       "http://example.com",
			request:       RequestWrapper{Args: mockURIArgs{"testNode": {"invalidaction()"}}},
			nodeName:      "testNode",
			correlationID: "12345",
			expectedErr:   errors.New("PerformActions: got wrong action invalidaction"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tb := &TimestampBuilder{}
			_, headers, status, err := PerformActions(tt.baseURL, tt.request, tt.nodeName, tb, tt.correlationID)

			if tt.expectedErr != nil {
				assert.Error(t, err)
				assert.Equal(t, tt.expectedErr.Error(), err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedHeaders, headers)
				assert.Equal(t, tt.expectedStatus, status)
			}
		})
	}
}
