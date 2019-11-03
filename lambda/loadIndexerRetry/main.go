package main

import (
	"context"
	"os"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

func handleRequest(ctx context.Context, event interface{}) error {
	logger.WithField("event", event).Info("Start loadIndexerRetry")

	return nil
}

func main() {
	logger.SetLevel(logrus.InfoLevel)
	logger.SetFormatter(&logrus.JSONFormatter{})
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))
	lambda.Start(handleRequest)
}
