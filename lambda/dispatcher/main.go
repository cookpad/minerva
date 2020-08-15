package main

import (
	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/pkg/lambda"
)

var logger = lambda.Logger

func main() {
	lambda.StartHandler(handler)
}

func handler(args lambda.HandlerArguments) error {
	var event events.DynamoDBEvent
	if err := args.DecodeEvent(&event); err != nil {
		return err
	}

	logger.WithField("event", event).Info("waiwai")

	return nil
}
