package benchmark

import (
	"testing"
)

func TestNewLogger(t *testing.T) {
	l := NewLogger(1)
	if l.LogLevel != 1 {
		t.Errorf("NewLogger() error, log level = %v, want %v", l.LogLevel, 1)
	}
}

func TestLogMsg(t *testing.T) {
	l := NewLogger(1)
	msg, skip := l.LogMsg(1, 1, "test message")
	if skip {
		t.Errorf("LogMsg() error, message was skipped")
	}
	if msg == "" {
		t.Errorf("LogMsg() error, message is empty")
	}
}
