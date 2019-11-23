package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const hardLimitOfSearchResult = 1000 * 1000 // 1,000,000

type startQueryExecutionResponse struct {
	QueryID string `json:"query_id"`
}

type query struct {
	Term  string `json:"term"`
	Field string `json:"field,omitempty"`
}

type request struct {
	Query         []query `json:"query"`
	StartDateTime string  `json:"start_dt"`
	EndDateTime   string  `json:"end_dt"`
}

func execSearch(args arguments) (*events.APIGatewayProxyResponse, apiError) {
	logger.WithField("args", args).Info("Start putQuery")

	var req request
	if err := json.Unmarshal([]byte(args.Request.Body), &req); err != nil {
		return nil, wrapUserError(err, 400, "Fail to parse requested body")
	}

	sql, err := buildSQL(req, args.IndexTableName, args.MessageTableName)
	if err != nil {
		return nil, wrapUserError(err, 400, "Fail to create SQL for Athena")
	}

	ssn := session.Must(session.NewSession(&aws.Config{Region: &args.Region}))
	athenaClient := athena.New(ssn)

	input := &athena.StartQueryExecutionInput{
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String(args.DatabaseName),
		},
		QueryString: sql,
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: &args.OutputPath,
		},
	}

	logger.WithField("input", input).Info("Athena Query")

	response, err := athenaClient.StartQueryExecution(input)
	logger.WithFields(logrus.Fields{
		"err":    err,
		"input":  input,
		"output": response,
	}).Debug("done")

	if err != nil {
		return nil, wrapSystemError(err, 500, "Fail StartQueryExecution in putQuery")
	}
	logger.WithField("response", response).Debug("Sent query")

	return respond(201, &startQueryExecutionResponse{
		QueryID: aws.StringValue(response.QueryExecutionId),
	}), nil
}

func buildSQL(req request, idxTable, msgTable string) (*string, error) {
	tokenizer := internal.NewSimpleTokenizer()

	termSet := map[string]struct{}{}

	if len(req.Query) == 0 {
		return nil, fmt.Errorf("No query. 'query' field is required")
	}

	for _, q := range req.Query {
		tokens := tokenizer.Split(q.Term)
		for _, t := range tokens {
			if t.IsDelim {
				continue
			}

			termSet[t.Data] = struct{}{}
		}
	}

	var termCond []string
	for t := range termSet {
		termCond = append(termCond, fmt.Sprintf("indices.term = '%s'", t))
	}

	// TODO: replace LIKE with regex feature
	var queryCond []string
	for _, q := range req.Query {
		queryCond = append(queryCond, fmt.Sprintf("messages.message LIKE '%%%s%%'", q.Term))
	}

	dtFmt := "2006-01-02-15"
	inputFmt := "2006-01-02T15:04:05"

	start, err := time.Parse(inputFmt, req.StartDateTime)
	if err != nil {
		return nil, errors.Wrapf(err, "Invalid start_dt format: %v", req.StartDateTime)
	}
	end, err := time.Parse(inputFmt, req.EndDateTime)
	if err != nil {
		return nil, errors.Wrapf(err, "Invalid end_dt format: %v", req.EndDateTime)
	}

	idxTerms := strings.Join(termCond, "\nOR ")
	idxWhere := fmt.Sprintf(
		"'%s' <= indices.dt \n"+
			"AND indices.dt <= '%s' \n"+
			"AND %d <= indices.timestamp \n"+
			"AND indices.timestamp <= %d \n"+
			"AND (%s)",
		start.Format(dtFmt), end.Format(dtFmt),
		start.Unix(), end.Unix(),
		idxTerms)
	msgTerms := strings.Join(queryCond, " AND ")
	msgWhere := fmt.Sprintf("'%s' <= messages.dt \nAND messages.dt <= '%s' \nAND %s",
		start.Format(dtFmt), end.Format(dtFmt), msgTerms)

	sql := fmt.Sprintf(`WITH tindex AS (
SELECT indices.object_id, indices.seq, indices.tag
FROM indices
WHERE %s
GROUP BY indices.object_id, indices.seq, indices.tag, indices.timestamp
HAVING count(distinct(field, term)) = %d
)
SELECT tindex.tag,
messages.timestamp,
messages.message
FROM messages
RIGHT JOIN tindex
ON messages.object_id = tindex.object_id
AND messages.seq = tindex.seq
WHERE %s
ORDER BY messages.timestamp`, idxWhere, len(termSet), msgWhere)

	return &sql, nil
}
