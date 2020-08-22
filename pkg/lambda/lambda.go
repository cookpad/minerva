package lambda

import (
	"context"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Logger is common logger gateway
var Logger = internal.Logger

// Handler has main logic of the lambda function
type Handler func(HandlerArguments) error

// StartHandler initialize AWS Lambda and invokes handler
func StartHandler(handler Handler) {
	Logger.SetLevel(logrus.InfoLevel)
	Logger.SetFormatter(&logrus.JSONFormatter{})

	lambda.Start(func(ctx context.Context, event interface{}) error {
		defer internal.FlushError()

		var args HandlerArguments
		if err := args.BindEnvVars(); err != nil {
			internal.HandleError(err)
			return err
		}

		if args.LogLevel != "" {
			internal.SetLogLevel(args.LogLevel)
		}

		Logger.WithFields(logrus.Fields{"args": args, "event": event}).Debug("Start handler")
		args.Event = event

		if err := handler(args); err != nil {
			Logger.WithFields(logrus.Fields{"args": args, "event": event}).Error("Failed Handler")
			err = errors.Wrap(err, "Failed Handler")
			internal.HandleError(err)
			return err
		}

		return nil
	})
}
