package internal

import (
	"os"
	"time"

	sentry "github.com/getsentry/sentry-go"
)

var sentryEnabled = false

func initErrorHandler() {
	if os.Getenv("SENTRY_DSN") != "" {
		err := sentry.Init(sentry.ClientOptions{
			Dsn: "",
			// Debug: true,
		})
		if err != nil {
			Logger.WithError(err).Fatal("sentry.Init")
		}
		sentryEnabled = true
	}
}

// HandleError sends error to sentry if sentry configuration is available
func HandleError(err error) {
	r := Logger.WithError(err)

	if sentryEnabled {
		eventID := sentry.CaptureException(err)
		if eventID != nil {
			r = r.WithField("sentry eventID", *eventID)
		}
	}

	r.Error("Error")
}

// FlushError flushs error to sentry
func FlushError() {
	if sentryEnabled {
		sentry.Flush(2 * time.Second)
	}
}
