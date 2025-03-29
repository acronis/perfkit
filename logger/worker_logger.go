package logger

import (
	"fmt"
)

type WorkerLogger struct {
	PlaneLogger
	workerID int
}

func NewWorkerLogger(level LogLevel, storeLastMessage bool, workerID int) Logger {
	planeLogger, ok := NewPlaneLogger(level, storeLastMessage).(*PlaneLogger)
	if !ok {
		return nil
	}

	workerLogger := &WorkerLogger{}
	workerLogger.PlaneLogger = *planeLogger
	workerLogger.workerID = workerID
	return workerLogger
}

func (l *WorkerLogger) Log(level LogLevel, message string, args ...interface{}) {
	msg := fmt.Sprintf(message, args...)
	if l.workerID == -1 {
		msg = fmt.Sprintf("root process: %s", msg)
	} else {
		msg = fmt.Sprintf("worker # %03d: %s", l.workerID, msg)
	}
	l.PlaneLogger.Log(level, msg)
}

// Error logs an error message
func (l *WorkerLogger) Error(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelError, message)
}

// Warn logs a warning message
func (l *WorkerLogger) Warn(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelWarn, message)
}

// Info logs an informational message
func (l *WorkerLogger) Info(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelInfo, message)
}

// Debug logs a debug message
func (l *WorkerLogger) Debug(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelDebug, message)
}

// Trace logs a trace message
func (l *WorkerLogger) Trace(format string, args ...interface{}) {
	message := fmt.Sprintf(format, args...)
	l.Log(LevelTrace, message)
}

func (l *WorkerLogger) GetLastMessage() *LogMessage {
	// Delegate to the embedded PlaneLogger
	return l.PlaneLogger.GetLastMessage()
}

func (l *WorkerLogger) Clone() Logger {
	return NewWorkerLogger(l.GetLevel(), l.storeLastMsg, l.workerID)
}
