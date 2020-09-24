package main

import (
	"fmt"
	"os"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/internal"
	"github.com/sirupsen/logrus"

	ginadapter "github.com/awslabs/aws-lambda-go-api-proxy/gin"
	"github.com/gin-gonic/gin"
	"github.com/m-mizutani/minerva/pkg/api"
)

var logger = internal.Logger

func main() {
	logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetLevel(logrus.InfoLevel)
	internal.SetupLogger(os.Getenv("LOG_LEVEL"))

	args := api.MinervaHandler{
		DatabaseName:     os.Getenv("ATHENA_DB_NAME"),
		IndexTableName:   os.Getenv("INDEX_TABLE_NAME"),
		MessageTableName: os.Getenv("MESSAGE_TABLE_NAME"),
		OutputPath:       fmt.Sprintf("s3://%s/%soutput", os.Getenv("S3_BUCKET"), os.Getenv("S3_PREFIX")),
		Region:           os.Getenv("AWS_REGION"),
		MetaTableName:    os.Getenv("META_TABLE_NAME"),
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.Default()
	v1 := r.Group("/api/v1")
	api.SetupRoute(v1, &args)
	ginLambda := ginadapter.New(r)

	lambda.Start(func(req events.APIGatewayProxyRequest) (events.APIGatewayProxyResponse, error) {
		return ginLambda.Proxy(req)
	})
}
