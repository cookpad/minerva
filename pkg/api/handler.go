package api

import (
	"encoding/json"

	"github.com/aws/aws-lambda-go/events"
)

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

	Logger.WithError(err)

	return events.APIGatewayProxyResponse{
		Body:       string(raw),
		StatusCode: err.Code(),
	}
}

type requestEntry struct {
	method   string
	resource string
}
