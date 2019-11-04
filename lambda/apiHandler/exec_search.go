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

	sql, err := argsToSQL(req, args.IndexTableName, args.MessageTableName)
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

func argsToSQL(req request, idxTable, msgTable string) (*string, error) {
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

	var queryCond []string
	for _, query := range req.Query {
		queryCond = append(queryCond, fmt.Sprintf("messages.message LIKE '%%%s%%'", query))
	}

	dtFmt := "2006-01-02"
	start, end := req.StartDateTime.UTC(), req.EndDateTime.UTC()

	head := fmt.Sprintf(`
	SELECT %s.tag
		, %s.timestamp
	 	, %s.message
	FROM %s
	LEFT JOIN %s
	ON %s.object_id = %s.object_id
		AND  %s.seq = %s.seq
	 `,
		idxTable,
		msgTable,
		msgTable,
		idxTable, msgTable,
		idxTable, msgTable,
		idxTable, msgTable)

	where := fmt.Sprintf(`WHERE (%s)
	AND %d <= %s.timestamp AND %s.timestamp <= %d
	AND '%s' <= %s.dt AND %s.dt <= '%s'
	AND '%s' <= %s.dt AND %s.dt <= '%s'
	`,
		strings.Join(termCond, " OR "),
		start.Unix(), idxTable, idxTable, end.Unix(),
		start.Format(dtFmt), idxTable, idxTable, end.Format(dtFmt),
		start.Format(dtFmt), msgTable, msgTable, end.Format(dtFmt))

	groupBy := fmt.Sprintf(`GROUP BY  %s.object_id, %s.seq, %s.tag, %s.message, %s.timestamp
	HAVING COUNT(DISTINCT %s.term) = %d
	AND %s
	ORDER BY messages.timestamp
	LIMIT %d;`,
		idxTable, idxTable, idxTable, msgTable, msgTable,
		idxTable, len(termCond),
		strings.Join(queryCond, " AND "),
		hardLimitOfSearchResult)

	sql := head + where + groupBy
	return &sql, nil
}
