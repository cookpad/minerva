package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/sirupsen/logrus"
)

var logger = internal.Logger

func respond(code int, msg interface{}) *events.APIGatewayProxyResponse {
	var body string

	switch msg.(type) {
	case string:
		v := struct {
			Message string `json:"message"`
		}{Message: msg.(string)}
		raw, _ := json.Marshal(v)
		body = string(raw)
	default:
		raw, _ := json.Marshal(msg)
		body = string(raw)
	}

	return &events.APIGatewayProxyResponse{
		Body:       body,
		StatusCode: code,
	}
}

func respondError(err apiError) events.APIGatewayProxyResponse {
	type respMessage struct {
		Message string `json:"message"`
	}

	r := respMessage{Message: err.Message()}
	raw, _ := json.Marshal(r)

	logger.WithError(err)

	return events.APIGatewayProxyResponse{
		Body:       string(raw),
		StatusCode: err.Code(),
	}
}

func main() {
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)
	internal.SetLogLevel(os.Getenv("LOG_LEVEL"))

	lambda.Start(func(request events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		logger.WithFields(logrus.Fields{
			"request": request,
		}).Info("entering handler")

		args := arguments{
			DatabaseName:     os.Getenv("ATHENA_DB_NAME"),
			IndexTableName:   os.Getenv("INDEX_TABLE_NAME"),
			MessageTableName: os.Getenv("MESSAGE_TABLE_NAME"),
			OutputPath:       fmt.Sprintf("s3://%s/%soutput", os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX")),
			Region:           os.Getenv("AWS_REGION"),
			Request:          request,
		}

		resp, err := handler(args)
		if err != nil {
			return respondError(err), nil
		}

		return *resp, nil
	})
}
