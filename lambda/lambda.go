package lambda

import (
	"context"
	"encoding/json"

	"github.com/Netflix/go-env"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Logger is common logger gateway
var Logger = internal.Logger

// Handler has main logic of the lambda function
type Handler func(HandlerArguments) error

// HandlerArguments has environment variables, Event record and adaptor
type HandlerArguments struct {
	EnvVars
	Event interface{}
}

// DecodeEvent marshals and unmarshal received lambda event to struct.
func (x *HandlerArguments) DecodeEvent(ev interface{}) error {
	raw, err := json.Marshal(x.Event)
	if err != nil {
		return errors.Wrap(err, "Failed to marshal lambda event in DecodeEvent")
	}

	if err := json.Unmarshal(raw, &ev); err != nil {
		Logger.WithField("raw", string(raw)).Error("json.Unmarshal")
		return errors.Wrap(err, "Failed json.Unmarshal in DecodeEvent")
	}

	return nil
}

// StartHandler initialize AWS Lambda and invokes handler
func StartHandler(handler Handler) {
	Logger.SetLevel(logrus.InfoLevel)
	Logger.SetFormatter(&logrus.JSONFormatter{})
	lambda.Start(func(ctx context.Context, event interface{}) error {
		defer internal.FlushError()

		var args HandlerArguments
		_, err := env.UnmarshalFromEnviron(&args)
		if err != nil {
			Logger.WithError(err).Error("Failed UnmarshalFromEviron")
			internal.HandleError(err)
			return err
		}

		if args.LogLevel != "" {
			internal.SetLogLevel(args.LogLevel)
		}

		Logger.WithFields(logrus.Fields{"args": args, "event": event}).Debug("Start handler")

		if err := handler(args); err != nil {
			Logger.WithFields(logrus.Fields{"args": args, "event": event}).Error("Failed Handler")
			err = errors.Wrap(err, "Failed Handler")
			internal.HandleError(err)
			return err
		}

		return nil
	})
}
