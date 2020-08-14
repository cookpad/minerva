package internal

import (
	"strings"

	"github.com/sirupsen/logrus"
)

// Logger can be modified by external for testing
var Logger = logrus.New()

// SetLogLevel changes internal.Logger log level (both of upper/lower case are acceptable). Choose [TRACE|DEBUG|INFO|WARN|ERROR].
func SetLogLevel(level string) {
	lv := strings.ToUpper(level)
	switch lv {
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
	default:
		Logger.Warnf("LogLevel '%s' is not supported", lv)
	}
}
