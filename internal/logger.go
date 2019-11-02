package internal

import (
	"github.com/sirupsen/logrus"
)

// Logger can be modified by external for testing
var Logger = logrus.New()

func SetLogLevel(level string) {
	switch level {
	case "TRACE":
		Logger.SetLevel(logrus.TraceLevel)
	case "DEBUG":
		Logger.SetLevel(logrus.DebugLevel)
	case "INFO":
		Logger.SetLevel(logrus.InfoLevel)
	case "WARN":
		Logger.SetLevel(logrus.WarnLevel)
	case "ERROR":
		Logger.SetLevel(logrus.ErrorLevel)
	}
}
