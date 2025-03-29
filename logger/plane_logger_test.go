package logger

import (
	"strings"
	"testing"
	"time"
)

func TestNewPlaneLogger(t *testing.T) {
	tests := []struct {
		name          string
		level         LogLevel
		storeLastMsg  bool
		expectedLevel LogLevel
		expectedStore bool
	}{
		{
			name:          "error level without storage",
			level:         LevelError,
			storeLastMsg:  false,
			expectedLevel: LevelError,
			expectedStore: false,
		},
		{
			name:          "debug level with storage",
			level:         LevelDebug,
			storeLastMsg:  true,
			expectedLevel: LevelDebug,
			expectedStore: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := NewPlaneLogger(tc.level, tc.storeLastMsg).(*PlaneLogger)

			if logger.GetLevel() != tc.expectedLevel {
				t.Errorf("Expected level %v, got %v", tc.expectedLevel, logger.GetLevel())
			}

			lastMsg := logger.GetLastMessage()
			if lastMsg != nil {
				t.Errorf("Expected storeLastMsg %v, got %v", tc.expectedStore, lastMsg)
			}
		})
	}
}

func TestPlaneLogger_GetSetLevel(t *testing.T) {
	logger := NewPlaneLogger(LevelInfo, false).(*PlaneLogger)

	if logger.GetLevel() != LevelInfo {
		t.Errorf("Initial level not set correctly, expected %v, got %v", LevelInfo, logger.GetLevel())
	}

	logger.SetLevel(LevelDebug)
	if logger.GetLevel() != LevelDebug {
		t.Errorf("Level not changed correctly, expected %v, got %v", LevelDebug, logger.GetLevel())
	}
}

func TestPlaneLogger_LogMethods(t *testing.T) {
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
			expected: "test error 1",
		},
		{
			name: "warn message",
			logFunc: func(l Logger) {
				l.Warn("test warning %s", "msg")
			},
			level:    LevelWarn,
			expected: "test warning msg",
		},
		{
			name: "info message",
			logFunc: func(l Logger) {
				l.Info("test info")
			},
			level:    LevelInfo,
			expected: "test info",
		},
		{
			name: "debug message",
			logFunc: func(l Logger) {
				l.Debug("test debug")
			},
			level:    LevelDebug,
			expected: "test debug",
		},
		{
			name: "trace message",
			logFunc: func(l Logger) {
				l.Trace("test trace")
			},
			level:    LevelTrace,
			expected: "test trace",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			logger := NewPlaneLogger(LevelTrace, true).(*PlaneLogger)
			tc.logFunc(logger)

			lastMsg := logger.GetLastMessage()
			var level LogLevel
			var msg string
			var timestamp time.Time
			var ok bool
			if lastMsg != nil {
				level = lastMsg.Level
				msg = lastMsg.Message
				timestamp = lastMsg.Time
				ok = true
			}
			if !ok {
				t.Error("Expected to get stored message")
			}
			if level != tc.level {
				t.Errorf("Expected level %v, got %v", tc.level, level)
			}
			if !strings.Contains(msg, tc.expected) {
				t.Errorf("Expected message to contain %q, got %q", tc.expected, msg)
			}
			if time.Since(timestamp) > time.Minute {
				t.Errorf("Timestamp seems incorrect: %v", timestamp)
			}
		})
	}
}

func TestPlaneLogger_LogLevelFiltering(t *testing.T) {
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
			logger := NewPlaneLogger(tc.loggerLevel, true).(*PlaneLogger)
			tc.messageFn(logger)

			lastMsg := logger.GetLastMessage()
			ok := lastMsg != nil
			if ok != tc.shouldContain {
				if tc.shouldContain {
					t.Errorf("Expected output for %s message at %s level, got none",
						tc.messageLevel, tc.loggerLevel)
				} else {
					t.Errorf("Expected no output for %s message at %s level, got message",
						tc.messageLevel, tc.loggerLevel)
				}
			}
		})
	}
}

func TestPlaneLogger_Clone(t *testing.T) {
	// Create original logger
	originalLogger := NewPlaneLogger(LevelWarn, true).(*PlaneLogger)

	// Clone it
	clonedLogger := originalLogger.Clone().(*PlaneLogger)

	// Verify properties are maintained
	if clonedLogger.GetLevel() != LevelWarn {
		t.Errorf("Expected cloned logger level %v, got %v", LevelWarn, clonedLogger.GetLevel())
	}

	// Verify independence - changing level of clone doesn't affect original
	clonedLogger.SetLevel(LevelTrace)
	if originalLogger.GetLevel() != LevelWarn {
		t.Errorf("Original logger level changed after modifying clone")
	}
}
