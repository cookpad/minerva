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

func respond(code int, msg interface{}) events.APIGatewayProxyResponse {
	raw, _ := json.Marshal(msg)
	return events.APIGatewayProxyResponse{
		Body:       string(raw),
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

		switch request.Path {
		case "/v1/query":
			switch request.HTTPMethod {
			case "POST":
				args := startQueryExecutionArgument{
					DatabaseName:     os.Getenv("ATHENA_DB_NAME"),
					IndexTableName:   os.Getenv("INDEX_TABLE_NAME"),
					MessageTableName: os.Getenv("MESSAGE_TABLE_NAME"),
					Output:           fmt.Sprintf("s3://%s/%soutput", os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX")),
					Region:           os.Getenv("AWS_REGION"),
					Body:             request.Body,
				}
				resp, err := startQueryExecution(args)
				if err != nil {
					return respondError(err), nil
				}
				return respond(201, resp), nil

			case "GET":
				args := getQueryExecutionArgument{
					DatabaseName: os.Getenv("ATHENA_DB_NAME"),
					Region:       os.Getenv("AWS_REGION"),
					QueryID:      request.QueryStringParameters["query_id"],
					Limit:        request.QueryStringParameters["limit"],
					Offset:       request.QueryStringParameters["offset"],
				}
				resp, err := getQueryExecution(args)
				if err != nil {
					return respondError(err), nil
				}
				return respond(200, resp), nil
			}
		}

		return respond(200, "ok"), nil
	})
}
