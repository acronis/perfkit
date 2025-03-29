package logger

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

// captureOutput captures stdout output for testing
func captureOutput(f func()) string {
	orig := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	f()

	w.Close()
	os.Stdout = orig

	var buf bytes.Buffer
	io.Copy(&buf, r)
	return buf.String()
}

func TestNewWorkerLogger(t *testing.T) {
	tests := []struct {
		name          string
		level         LogLevel
		storeLastMsg  bool
		workerID      int
		expectedLevel LogLevel
		expectedStore bool
	}{
		{
			name:          "error level without storage",
			level:         LevelError,
			storeLastMsg:  false,
			workerID:      1,
			expectedLevel: LevelError,
			expectedStore: false,
		},
		{
			name:          "debug level with storage",
			level:         LevelDebug,
			storeLastMsg:  true,
			workerID:      2,
			expectedLevel: LevelDebug,
			expectedStore: true,
		},
		{
			name:          "root process worker",
			level:         LevelInfo,
			storeLastMsg:  true,
			workerID:      -1,
			expectedLevel: LevelInfo,
			expectedStore: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := NewWorkerLogger(tc.level, tc.storeLastMsg, tc.workerID)

			if logger.GetLevel() != tc.expectedLevel {
				t.Errorf("Expected level %v, got %v", tc.expectedLevel, logger.GetLevel())
			}

			// Ensure the log level is set to at least INFO to allow the test message to be displayed
			logger.SetLevel(LevelInfo)

			// Test that the worker ID is correctly set by checking message formatting
			output := captureOutput(func() {
				logger.Info("test message")
			})

			if tc.workerID == -1 {
				if !strings.Contains(output, "root process: test message") {
					t.Errorf("Expected root process message format, got: %s", output)
				}
			} else {
				if !strings.Contains(output, "worker #") {
					t.Errorf("Expected worker prefix in message, got: %s", output)
				}
			}
		})
	}
}

func TestWorkerLogger_LogMethods(t *testing.T) {
	tests := []struct {
		name     string
		logFunc  func(l Logger)
		level    LogLevel
		expected string
	}{
		{
			name: "error message",
			logFunc: func(l Logger) {
				l.Error("test error %d", 1)
			},
			level:    LevelError,
			expected: "worker # 001: test error 1",
		},
		{
			name: "warn message",
			logFunc: func(l Logger) {
				l.Warn("test warning %s", "msg")
			},
			level:    LevelWarn,
			expected: "worker # 001: test warning msg",
		},
		{
			name: "info message",
			logFunc: func(l Logger) {
				l.Info("test info")
			},
			level:    LevelInfo,
			expected: "worker # 001: test info",
		},
		{
			name: "debug message",
			logFunc: func(l Logger) {
				l.Debug("test debug")
			},
			level:    LevelDebug,
			expected: "worker # 001: test debug",
		},
		{
			name: "trace message",
			logFunc: func(l Logger) {
				l.Trace("test trace")
			},
			level:    LevelTrace,
			expected: "worker # 001: test trace",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := NewWorkerLogger(LevelTrace, false, 1)

			output := captureOutput(func() {
				tc.logFunc(logger)
			})

			if !strings.Contains(output, tc.expected) {
				t.Errorf("Expected output to contain %q, got %q", tc.expected, output)
			}
		})
	}
}

func TestWorkerLogger_LogLevelFiltering(t *testing.T) {
	tests := []struct {
		name          string
		loggerLevel   LogLevel
		messageLevel  LogLevel
		messageFn     func(l Logger)
		shouldContain bool
	}{
		{
			name:          "error shown at error level",
			loggerLevel:   LevelError,
			messageLevel:  LevelError,
			messageFn:     func(l Logger) { l.Error("test") },
			shouldContain: true,
		},
		{
			name:          "warn hidden at error level",
			loggerLevel:   LevelError,
			messageLevel:  LevelWarn,
			messageFn:     func(l Logger) { l.Warn("test") },
			shouldContain: false,
		},
		{
			name:          "debug shown at debug level",
			loggerLevel:   LevelDebug,
			messageLevel:  LevelDebug,
			messageFn:     func(l Logger) { l.Debug("test") },
			shouldContain: true,
		},
		{
			name:          "trace hidden at debug level",
			loggerLevel:   LevelDebug,
			messageLevel:  LevelTrace,
			messageFn:     func(l Logger) { l.Trace("test") },
			shouldContain: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := NewWorkerLogger(tc.loggerLevel, false, 5)

			output := captureOutput(func() {
				tc.messageFn(logger)
			})

			hasOutput := output != ""
			if hasOutput != tc.shouldContain {
				if tc.shouldContain {
					t.Errorf("Expected output for %s message at %s level, got none",
						tc.messageLevel, tc.loggerLevel)
				} else {
					t.Errorf("Expected no output for %s message at %s level, got: %s",
						tc.messageLevel, tc.loggerLevel, output)
				}
			}
		})
	}
}

func TestWorkerLogger_GetLastMessage(t *testing.T) {
	// Test with storage disabled
	logger := NewWorkerLogger(LevelInfo, false, 42)
	logger.Info("This message should not be stored")
	lastMsg := logger.GetLastMessage()
	if lastMsg != nil {
		t.Errorf("Expected no stored message when storage is disabled")
	}

	// Test with storage enabled
	logger = NewWorkerLogger(LevelInfo, true, 42)
	testMsg := "This message should be stored"
	logger.Info(testMsg)
	lastMsg = logger.GetLastMessage()
	if lastMsg == nil {
		t.Errorf("GetLastMessage failed, expected a message")
		return
	}
	if lastMsg.Level != LevelInfo {
		t.Errorf("Expected level %v, got %v", LevelInfo, lastMsg.Level)
	}
	if !strings.Contains(lastMsg.Message, testMsg) {
		t.Errorf("Expected message to contain %q, got %q", testMsg, lastMsg.Message)
	}
	if time.Since(lastMsg.Time) > time.Minute {
		t.Errorf("Timestamp seems incorrect: %v", lastMsg.Time)
	}
}

func TestWorkerLogger_Clone(t *testing.T) {
	// Create original logger with trace level to ensure messages are displayed
	originalLogger := NewWorkerLogger(LevelTrace, true, 99)

	// Clone it
	clonedLogger := originalLogger.Clone()

	// Verify properties are maintained
	if clonedLogger.GetLevel() != LevelTrace {
		t.Errorf("Expected cloned logger level %v, got %v", LevelTrace, clonedLogger.GetLevel())
	}

	// Test that the worker ID is correctly cloned by checking message formatting
	output := captureOutput(func() {
		clonedLogger.Info("test message")
	})

	if !strings.Contains(output, "worker # 099:") {
		t.Errorf("Expected worker ID to be preserved in clone, got: %s", output)
	}

	// Verify independence - changing level of clone doesn't affect original
	clonedLogger.SetLevel(LevelError)
	if originalLogger.GetLevel() != LevelTrace {
		t.Errorf("Original logger level changed after modifying clone")
	}
}

func TestWorkerLogger_RootProcess(t *testing.T) {
	// Test root process (-1 worker ID)
	logger := NewWorkerLogger(LevelInfo, false, -1)

	output := captureOutput(func() {
		logger.Info("test message")
	})

	if !strings.Contains(output, "root process: test message") {
		t.Errorf("Expected root process message format, got: %s", output)
	}
}

func TestWorkerLogger_LogFormatting(t *testing.T) {
	// Test with regular worker ID
	logger := NewWorkerLogger(LevelInfo, false, 7)

	output := captureOutput(func() {
		logger.Info("test message")
	})

	// Verify timestamp format and worker ID prefix
	if !strings.Contains(output, "INF") {
		t.Errorf("Expected 'INF' in log output: %s", output)
	}
	if !strings.Contains(output, "worker # 007: test message") {
		t.Errorf("Expected worker ID prefix and message in log output: %s", output)
	}
}
