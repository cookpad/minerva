package main

import (
	"strings"

	"github.com/aws/aws-lambda-go/events"
)

type arguments struct {
	DatabaseName     string
	IndexTableName   string
	MessageTableName string
	OutputPath       string
	Region           string
	Request          events.APIGatewayProxyRequest
}

type requestHandler func(args arguments) (*events.APIGatewayProxyResponse, apiError)

type requestEntry struct {
	method   string
	resource string
}

var apiRoute = map[requestEntry]requestHandler{
	requestEntry{"POST", "/api/v1/search"}:                     execSearch,
	requestEntry{"GET", "/api/v1/search/{search_id}/result"}:   getSearchResult,
	requestEntry{"GET", "/api/v1/search/{search_id}/timeline"}: getSearchTimeline,
	requestEntry{"DELETE", "/api/v1/search/{search_id}"}:       cancelSearch,
}

func handler(args arguments) (*events.APIGatewayProxyResponse, apiError) {
	reqEntry := requestEntry{
		method:   strings.ToUpper(args.Request.HTTPMethod),
		resource: args.Request.Resource,
	}

	reqHandler, ok := apiRoute[reqEntry]
	if !ok {
		return nil, newUserErrorf(404, "No matched route for %v", reqHandler)
	}
	logger.WithField("matchedEntry", reqHandler).Info("Route matched")

	return reqHandler(args)
}

func cancelSearch(args arguments) (*events.APIGatewayProxyResponse, apiError) {
	return respond(200, "ok"), nil
}
