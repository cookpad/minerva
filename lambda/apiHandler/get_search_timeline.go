package main

import "github.com/aws/aws-lambda-go/events"

func getSearchTimeline(args arguments) (*events.APIGatewayProxyResponse, apiError) {
	return respond(200, "ok"), nil
}
