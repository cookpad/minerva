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
	"github.com/sirupsen/logrus"
)

const hardLimitOfSearchResult = 1000 * 1000 // 1,000,000

type startQueryExecutionResponse struct {
	SearchID string `json:"search_id"`
}

type request struct {
	Query         []string  `json:"query"`
	StartDateTime time.Time `json:"start_dt"`
	EndDateTime   time.Time `json:"end_dt"`
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
		SearchID: aws.StringValue(response.QueryExecutionId),
	}), nil
}

func buildSQL(req request, idxTable, msgTable string) (*string, error) {
	tokenizer := internal.NewSimpleTokenizer()

	termSet := map[string]struct{}{}

	if len(req.Query) == 0 {
		return nil, fmt.Errorf("No query. Query field is required")
	}

	for _, query := range req.Query {
		tokens := tokenizer.Split(query)
		for _, t := range tokens {
			if t.IsDelim {
				continue
			}

			termSet[t.Data] = struct{}{}
		}
	}

	var termCond []string
	for t := range termSet {
		termCond = append(termCond, fmt.Sprintf("term = '%s'", t))
	}

	// TODO: replace LIKE with regex feature
	var queryCond []string
	for _, query := range req.Query {
		queryCond = append(queryCond, fmt.Sprintf("messages.message LIKE '%%%s%%'", query))
	}

	dtFmt := "2006-01-02-15"
	start, end := req.StartDateTime.UTC(), req.EndDateTime.UTC()

	idxTerms := strings.Join(termCond, " OR ")
	idxWhere := fmt.Sprintf(`'%s' <= dt AND dt <= '%s' AND (%s)`,
		start.Format(dtFmt), end.Format(dtFmt), idxTerms)
	msgTerms := strings.Join(queryCond, " AND ")
	msgWhere := fmt.Sprintf(`'%s' <= messages.dt AND messages.dt <= '%s' AND %s`,
		start.Format(dtFmt), end.Format(dtFmt), msgTerms)

	sql := fmt.Sprintf(`with tindex AS (
		SELECT object_id, seq, tag
			FROM indices
			WHERE %s
		GROUP BY  object_id, seq, tag, timestamp
		HAVING count(distinct(field, term)) = %d
	)
	SELECT messages.timestamp,
		tindex.tag,
		messages.message
	FROM messages
	LEFT JOIN tindex
	ON messages.object_id = tindex.object_id
		AND messages.seq = tindex.seq
		WHERE %s
		ORDER BY messages.timestamp
	`, idxWhere, len(idxTerms), msgWhere)

	return &sql, nil
}
