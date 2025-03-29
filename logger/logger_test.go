package logger

import (
	"testing"
)

func TestNewLogger(t *testing.T) {
	l := NewPlaneLogger(LevelWarn, false)
	if l.GetLevel() != LevelWarn {
		t.Errorf("NewPlaneLogger() error, log level = %v, want %v", l.GetLevel(), LevelWarn)
	}
}

func TestLogMessage(t *testing.T) {
	l := NewPlaneLogger(LevelWarn, true)
	l.Log(LevelWarn, "test message")
	msg := l.GetLastMessage()
	if msg == nil {
		t.Errorf("Log() error, message was not stored")
	}
	if msg != nil && msg.Message == "" {
		t.Errorf("Log() error, message is empty")
	}
}
