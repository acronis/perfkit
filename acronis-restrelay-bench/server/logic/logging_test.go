package logic

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
func init() {
	logfLoggerJSON = newMockLogfLogger(false)
	logfLoggerText = newMockLogfLogger(true)
	logrusLoggerJSON = newMockLogrusLogger(&logrus.JSONFormatter{})
	logrusLoggerText = newMockLogrusLogger(&logrus.TextFormatter{})
}

func newMockLogfLogger(useText bool) *logf.Logger {
	if useText {
		writer, _ := logf.NewChannelWriter(logf.ChannelWriterConfig{
			Appender: logftext.NewAppender(bytes.NewBuffer([]byte{}), logftext.EncoderConfig{}),
		})
		return logf.NewLogger(logf.LevelInfo, writer)
	}
	writer, _ := logf.NewChannelWriter.Default()
	return logf.NewLogger(logf.LevelInfo, writer)
}

func newMockLogrusLogger(formatter logrus.Formatter) *logrus.Logger {
	logger := logrus.New()
	logger.Level = logrus.InfoLevel
	logger.Formatter = formatter
	logger.Out = bytes.NewBuffer([]byte{})
	return logger
}

func captureLogOutput(logger *logrus.Logger, f func()) string {
	var buf bytes.Buffer
	logger.SetOutput(&buf)
	f()
	return buf.String()
}

func TestLoggingAction_Perform(t *testing.T) {
	tests := []struct {
		name     string
		action   loggingAction
		expected string
	}{
		// Valid cases for logf logger in text format
		{
			name: "logf text warn",
			action: loggingAction{
				Skip:       false,
				LoggerType: loggerLogf,
				LogType:    logText,
				Length:     2,
			},
			expected: "WARN log message\nWARN log message\n",
		},
		{
			name: "logf text debug",
			action: loggingAction{
				Skip:       true,
				LoggerType: loggerLogf,
				LogType:    logText,
				Length:     2,
			},
			expected: "DEBUG log message\nDEBUG log message\n",
		},
		// Valid cases for logf logger in JSON format
		{
			name: "logf json warn",
			action: loggingAction{
				Skip:       false,
				LoggerType: loggerLogf,
				LogType:    logJSON,
				Length:     2,
			},
			expected: "{\"level\":\"warn\",\"message\":\"log message\",\"timestamp\":",
		},
		{
			name: "logf json debug",
			action: loggingAction{
				Skip:       true,
				LoggerType: loggerLogf,
				LogType:    logJSON,
				Length:     2,
			},
			expected: "{\"level\":\"debug\",\"message\":\"log message\",\"timestamp\":",
		},
		// Valid cases for logrus logger in text format
		{
			name: "logrus text warn",
			action: loggingAction{
				Skip:       false,
				LoggerType: loggerLogrus,
				LogType:    logText,
				Length:     2,
			},
			expected: "level=warn msg=\"log message\" timestamp=",
		},
		{
			name: "logrus text debug",
			action: loggingAction{
				Skip:       true,
				LoggerType: loggerLogrus,
				LogType:    logText,
				Length:     2,
			},
			expected: "level=debug msg=\"log message\" timestamp=",
		},
		// Valid cases for logrus logger in JSON format
		{
			name: "logrus json warn",
			action: loggingAction{
				Skip:       false,
				LoggerType: loggerLogrus,
				LogType:    logJSON,
				Length:     2,
			},
			expected: "{\"level\":\"warn\",\"msg\":\"log message\",\"timestamp\":",
		},
		{
			name: "logrus json debug",
			action: loggingAction{
				Skip:       true,
				LoggerType: loggerLogrus,
				LogType:    logJSON,
				Length:     2,
			},
			expected: "{\"level\":\"debug\",\"msg\":\"log message\",\"timestamp\":",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var output string
			switch tt.action.LoggerType {
			case loggerLogf:
				if tt.action.LogType == logJSON {
					output = captureLogOutput(logrusLoggerJSON, func() {
						tt.action.perform()
					})
				} else {
					output = captureLogOutput(logrusLoggerText, func() {
						tt.action.perform()
					})
				}
			case loggerLogrus:
				if tt.action.LogType == logJSON {
					output = captureLogOutput(logrusLoggerJSON, func() {
						tt.action.perform()
					})
				} else {
					output = captureLogOutput(logrusLoggerText, func() {
						tt.action.perform()
					})
				}
			}
			assert.Contains(t, output, tt.expected)
		})
	}
}

*/

func TestLoggingAction_ParseParameters(t *testing.T) {
	tests := []struct {
		params      map[string]string
		expected    loggingAction
		expectError bool
		errMessage  string
	}{
		// Valid cases
		{
			params: map[string]string{
				"skip":   "true",
				"logger": "logf",
				"type":   "text",
				"length": "10",
			},
			expected: loggingAction{
				Skip:       true,
				LoggerType: loggerLogf,
				LogType:    logText,
				Length:     10,
			},
			expectError: false,
		},
		{
			params: map[string]string{
				"skip":   "false",
				"logger": "logrus",
				"type":   "json",
				"length": "5",
			},
			expected: loggingAction{
				Skip:       false,
				LoggerType: loggerLogrus,
				LogType:    logJSON,
				Length:     5,
			},
			expectError: false,
		},
		// Invalid cases
		{
			params: map[string]string{
				"skip":   "invalid",
				"logger": "logf",
				"type":   "text",
				"length": "10",
			},
			expectError: true,
			errMessage:  "failed conversion string to bool for skip parameter: strconv.ParseBool: parsing \"invalid\": invalid syntax",
		},
		{
			params: map[string]string{
				"skip":   "true",
				"logger": "invalid",
				"type":   "text",
				"length": "10",
			},
			expectError: true,
			errMessage:  "unknown logger, should be logf for logf and logrus for logrus",
		},
		{
			params: map[string]string{
				"skip":   "true",
				"logger": "logf",
				"type":   "invalid",
				"length": "10",
			},
			expectError: true,
			errMessage:  "unknown log type, should be text for text and json for json",
		},
		{
			params: map[string]string{
				"skip":   "true",
				"logger": "logf",
				"type":   "text",
				"length": "0",
			},
			expectError: true,
			errMessage:  "number of log rows should be greater than 0",
		},
		{
			params:      map[string]string{"skip": "true", "logger": "logf", "type": "text"},
			expectError: true,
			errMessage:  "length parameter is missing",
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("params=%v", tt.params), func(t *testing.T) {
			action := &loggingAction{}
			err := action.parseParameters(tt.params)

			if tt.expectError {
				assert.Error(t, err)
				assert.Equal(t, tt.errMessage, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, *action)
			}
		})
	}
}
