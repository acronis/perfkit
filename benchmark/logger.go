package benchmark

import (
	"fmt"
	"time"
)

// Logger is a simple logger that can be used to log messages to stdout
type Logger struct {
	LogLevel int
}

// NewLogger creates a new Logger instance with the given log level
func NewLogger(logLevel int) *Logger {
	return &Logger{LogLevel: logLevel}
}

// LogMsg returns a formatted log message string and a boolean indicating whether the message should be skipped
func (l *Logger) LogMsg(LogLevel int, workerID int, format string, args ...interface{}) (string, bool) {
	if LogLevel > l.LogLevel {
		return "", true
	}

	now := time.Now()

	s := fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second(), now.Nanosecond()/1000)

	switch LogLevel {
	case LogError:
		s += "    ERR: "
	case LogWarn:
		s += "    WRN: "
	case LogInfo:
		s += "    INF: "
	case LogDebug:
		s += "    DBG: "
	default:
		s += "    TRA: "
	}

	s += fmt.Sprintf("worker %03d: %s", workerID, format)
	for _, arg := range args {
		s += fmt.Sprintf(", %s", arg)
	}

	return s, false
}

// Log logs a formatted log message to stdout if the log level is high enough
func (l *Logger) Log(LogLevel int, workerID int, format string, args ...interface{}) {
	msg, skip := l.LogMsg(LogLevel, workerID, format, args...)
	if skip {
		return
	}
	msg += "\n"
	fmt.Print(msg)
}

// Logn logs a formatted log message to stdout if the log level is high enough
func (l *Logger) Logn(LogLevel int, workerID int, format string, args ...interface{}) {
	msg, skip := l.LogMsg(LogLevel, workerID, format, args...)
	if skip {
		return
	}
	fmt.Print(msg)
}
