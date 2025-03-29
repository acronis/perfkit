package logger

type Logger interface {
	Log(level LogLevel, message string, args ...interface{})
	Error(format string, args ...interface{})
	Warn(format string, args ...interface{})
	Info(format string, args ...interface{})
	Debug(format string, args ...interface{})
	Trace(format string, args ...interface{})
	GetLevel() LogLevel
	SetLevel(level LogLevel)
	GetLastMessage() *LogMessage
	Clone() Logger
}
