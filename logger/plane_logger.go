package logger

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

// LogLevel represents the severity of a log message
type LogLevel int32

const (
	// LevelError represents error level messages
	LevelError LogLevel = 0
	// LevelWarn represents warning level messages
	LevelWarn LogLevel = 1
	// LevelInfo represents informational messages
	LevelInfo LogLevel = 2
	// LevelDebug represents debug messages
	LevelDebug LogLevel = 3
	// LevelTrace represents trace messages with high detail
	LevelTrace LogLevel = 4
)

// String converts a LogLevel to a string representation
func (l LogLevel) String() string {
	switch l {
	case LevelError:
		return "ERR"
	case LevelWarn:
		return "WRN"
	case LevelInfo:
		return "INF"
	case LevelDebug:
		return "DBG"
	case LevelTrace:
		return "TRA"
	default:
		return "???"
	}
}

// ANSI color codes
const (
	colorReset = "\033[0m"
	colorError = "\033[31m"
	colorWarn  = "\033[33m"
	colorInfo  = "\033[37m"
	colorDebug = "\033[34m"
	colorTrace = "\033[35m"
)

// Logger represents a loggerStruct with color support and log levels
type PlaneLogger struct {
	level        atomic.Int32               // log level
	useColors    bool                       // whether to use colors in output
	storeLastMsg bool                       // whether to store the last message
	lastMsg      atomic.Pointer[LogMessage] // last message printed
}

// LogMessage stores information about a log message
type LogMessage struct {
	Level   LogLevel
	Message string
	Time    time.Time
}

// NewPlaneLogger creates a new logger with the specified log level
func NewPlaneLogger(level LogLevel, storeLastMessage bool) Logger {
	// Check if stdout is redirected to a file
	fileInfo, _ := os.Stdout.Stat()
	useColors := (fileInfo.Mode() & os.ModeCharDevice) != 0

	logger := &PlaneLogger{
		useColors:    useColors,
		storeLastMsg: storeLastMessage,
	}
	logger.level.Store(int32(level))
	return logger
}

// GetLevel returns the current log level
func (l *PlaneLogger) GetLevel() LogLevel {
	return LogLevel(l.level.Load())
}

// SetLevel sets the log level
func (l *PlaneLogger) SetLevel(level LogLevel) {
	l.level.Store(int32(level))
}

// levelToColor returns the ANSI color code for the given log level
func (l *PlaneLogger) levelToColor(level LogLevel) string {
	if !l.useColors {
		return ""
	}

	switch level {
	case LevelError:
		return colorError
	case LevelWarn:
		return colorWarn
	case LevelInfo:
		return colorInfo
	case LevelDebug:
		return colorDebug
	case LevelTrace:
		return colorTrace
	default:
		return ""
	}
}

// logInternal is the internal logging function
func (l *PlaneLogger) print(level LogLevel, message string) {
	// If current level is lower than the message level, don't log
	if l.GetLevel() < level {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05.000000")
	prefix := fmt.Sprintf("%s  %s:", timestamp, level.String())

	color := l.levelToColor(level)
	resetColor := ""
	if color != "" {
		resetColor = colorReset
	}

	// Construct and print the log message
	output := fmt.Sprintf("%s%s %s%s", color, prefix, message, resetColor)
	fmt.Println(output)

	// Store the last message if enabled
	if l.storeLastMsg {
		l.lastMsg.Store(&LogMessage{
			Level:   level,
			Message: message,
			Time:    time.Now(),
		})
	}
}

// Log implements the logger.Logger interface
func (l *PlaneLogger) Log(level LogLevel, message string, args ...interface{}) {
	message = fmt.Sprintf(message, args...)
	l.print(level, message)
}

// Error logs an error message
func (l *PlaneLogger) Error(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelError, message)
}

// Warn logs a warning message
func (l *PlaneLogger) Warn(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelWarn, message)
}

// Info logs an informational message
func (l *PlaneLogger) Info(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelInfo, message)
}

// Debug logs a debug message
func (l *PlaneLogger) Debug(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelDebug, message)
}

// Trace logs a trace message
func (l *PlaneLogger) Trace(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelTrace, message)
}

// GetLastMessage returns the last logged message if storage is enabled
func (l *PlaneLogger) GetLastMessage() *LogMessage {
	if !l.storeLastMsg {
		return nil
	}

	lastMsg := l.lastMsg.Load()
	if lastMsg == nil {
		return nil
	}

	return lastMsg
}

func (l *PlaneLogger) Clone() Logger {
	return NewPlaneLogger(l.GetLevel(), l.storeLastMsg)
}
