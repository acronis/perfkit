package logic

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/ssgreg/logf"
	"github.com/ssgreg/logftext"
)

type loggerType string

const (
	loggerLogf   loggerType = "logf"
	loggerLogrus loggerType = "logrus"
)

type logType string

const (
	logText logType = "text"
	logJSON logType = "json"
)

type loggingAction struct {
	Skip       bool
	LoggerType loggerType
	LogType    logType
	Length     int
}

var logfLoggerJSON = newLogfLogger(false)
var logfLoggerText = newLogfLogger(true)

func newLogfLogger(useText bool) *logf.Logger {
	if useText {
		writer, _ := logf.NewChannelWriter(logf.ChannelWriterConfig{
			Appender: logftext.NewAppender(os.Stdout, logftext.EncoderConfig{}),
		})

		return logf.NewLogger(logf.LevelInfo, writer)
	}

	writer, _ := logf.NewChannelWriter.Default()

	return logf.NewLogger(logf.LevelInfo, writer)
}

var logrusLoggerJSON = newLogrusLogger(&logrus.JSONFormatter{})
var logrusLoggerText = newLogrusLogger(&logrus.TextFormatter{})

func newLogrusLogger(formatter logrus.Formatter) *logrus.Logger {
	logger := logrus.New()
	logger.Level = logrus.InfoLevel
	logger.Formatter = formatter

	return logger
}

func (loggingArgs *loggingAction) perform() error {
	skip := loggingArgs.Skip
	message := "log message"
	timestamp := time.Now().Nanosecond()

	switch loggingArgs.LoggerType {
	case loggerLogf:
		if loggingArgs.LogType == logJSON {
			if skip {
				for i := 0; i < loggingArgs.Length; i++ {
					logfLoggerJSON.Debug(message, logf.Int("timestamp", timestamp))
				}
			} else {
				for i := 0; i < loggingArgs.Length; i++ {
					logfLoggerJSON.Warn(message, logf.Int("timestamp", timestamp))
				}
			}
		} else {
			if skip {
				for i := 0; i < loggingArgs.Length; i++ {
					logfLoggerText.Debug(message, logf.Int("timestamp", timestamp))
				}
			} else {
				for i := 0; i < loggingArgs.Length; i++ {
					logfLoggerText.Warn(message, logf.Int("timestamp", timestamp))
				}
			}
		}

	case loggerLogrus:
		if loggingArgs.LogType == logJSON {
			if skip {
				for i := 0; i < loggingArgs.Length; i++ {
					logrusLoggerJSON.WithFields(logrus.Fields{
						"timestamp": timestamp,
					}).Debug(message)
				}
			} else {
				for i := 0; i < loggingArgs.Length; i++ {
					logrusLoggerJSON.WithFields(logrus.Fields{
						"timestamp": timestamp,
					}).Warn(message)
				}
			}
		} else {
			if skip {
				for i := 0; i < loggingArgs.Length; i++ {
					logrusLoggerText.WithFields(logrus.Fields{
						"timestamp": timestamp,
					}).Debug(message)
				}
			} else {
				for i := 0; i < loggingArgs.Length; i++ {
					logrusLoggerText.WithFields(logrus.Fields{
						"timestamp": timestamp,
					}).Warn(message)
				}
			}
		}
	}

	return nil
}

func (loggingArgs *loggingAction) parseParameters(params map[string]string) error {
	var skip, ok = params["skip"]
	if ok {
		var err error
		skipValue, err := strconv.ParseBool(skip)
		if err != nil {
			return fmt.Errorf("failed conversion string to bool for skip parameter: %v", err)
		}

		loggingArgs.Skip = skipValue
	}

	logger, ok := params["logger"]
	if !ok {
		return errors.New("logger parameter is missing")
	}

	switch logger {
	case string(loggerLogf):
		loggingArgs.LoggerType = loggerLogf
	case string(loggerLogrus):
		loggingArgs.LoggerType = loggerLogrus
	default:
		return fmt.Errorf("unknown logger, should be %s for logf and %s for logrus", loggerLogf, loggerLogrus)
	}

	logRecordType, ok := params["type"]
	if !ok {
		return errors.New("type parameter is missing")
	}

	switch logRecordType {
	case string(logText):
		loggingArgs.LogType = logText
	case string(logJSON):
		loggingArgs.LogType = logJSON
	default:
		return fmt.Errorf("unknown log type, should be %s for text and %s for json", logText, logJSON)
	}

	length, ok := params["length"]
	if !ok {
		return errors.New("length parameter is missing")
	}

	logLength, err := strconv.Atoi(length)
	if err != nil {
		return fmt.Errorf("failed conversion string to int for length parameter: %v", err)
	}

	if logLength > 0 {
		loggingArgs.Length = logLength
	} else {
		return errors.New("number of log rows should be greater than 0")
	}

	return nil
}
