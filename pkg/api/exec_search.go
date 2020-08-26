package api

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"github.com/m-mizutani/minerva/pkg/tokenizer"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const hardLimitOfSearchResult = 1000 * 1000 // 1,000,000

type ExecSearchResponse struct {
	SearchID searchID `json:"search_id"`
}

type Query struct {
	Term string `json:"term" dynamo:"term"`
}

type ExecSearchRequest struct {
	Query         []Query `json:"query"`
	StartDateTime string  `json:"start_dt"`
	EndDateTime   string  `json:"end_dt"`
}

const searchRowLimit = 1000 * 1000

func (x *MinervaHandler) ExecSearch(c *gin.Context) (*Response, Error) {
	Logger.WithField("context", *c).Info("Start putQuery")

	var req ExecSearchRequest
	body, err := ioutil.ReadAll(c.Request.Body)
	if err != nil {
		return nil, wrapSystemError(err, 500, "Fail to read body")
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, wrapUserError(err, 400, "Fail to parse requested body")
	}

	sql, err := buildSQL(req, x.IndexTableName, x.MessageTableName)
	if err != nil {
		return nil, wrapUserError(err, 400, "Fail to create SQL for Athena")
	}

	ssn := session.Must(session.NewSession(&aws.Config{Region: &x.Region}))
	athenaClient := athena.New(ssn)

	input := &athena.StartQueryExecutionInput{
		QueryExecutionContext: &athena.QueryExecutionContext{
			Database: aws.String(x.DatabaseName),
		},
		QueryString: sql,
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: &x.OutputPath,
		},
	}

	Logger.WithField("input", input).Info("Athena Query")

	response, err := athenaClient.StartQueryExecution(input)
	Logger.WithFields(logrus.Fields{
		"err":    err,
		"input":  input,
		"output": response,
	}).Debug("done")

	if err != nil {
		return nil, wrapSystemError(err, 500, "Fail StartQueryExecution in putQuery")
	}

	start, end, err := parseRequestTimes(req)
	if err != nil {
		return nil, wrapUserError(err, http.StatusBadRequest, err.Error())
	}

	now := time.Now().UTC()
	item := searchItem{
		ID:            searchID(uuid.New().String()),
		Status:        statusRunning,
		CreatedAt:     &now,
		StartTime:     *start,
		EndTime:       *end,
		Query:         req.Query,
		RequestID:     c.GetHeader("x-request-id"),
		AthenaQueryID: aws.StringValue(response.QueryExecutionId),
	}

	if err := x.newSearchRepo().put(&item); err != nil {
		return nil, wrapSystemErrorf(err, http.StatusInternalServerError, "Fail to put searchItem of ExecSearch")
	}

	return &Response{201, &ExecSearchResponse{
		SearchID: item.ID,
	}}, nil
}

func buildSQL(req ExecSearchRequest, idxTable, msgTable string) (*string, error) {
	tokenizer := tokenizer.NewSimpleTokenizer()

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
	start, end, err := parseRequestTimes(req)
	if err != nil {
		return nil, err
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
HAVING count(distinct(term)) = %d
LIMIT %d
)
SELECT tindex.tag,
messages.timestamp,
messages.message
FROM messages
RIGHT JOIN tindex
ON messages.object_id = tindex.object_id
AND messages.seq = tindex.seq
WHERE %s
ORDER BY messages.timestamp`,
		idxWhere, len(termSet), searchRowLimit, msgWhere)

	return &sql, nil
}

func parseRequestTimes(req ExecSearchRequest) (*time.Time, *time.Time, error) {
	inputFmt := "2006-01-02T15:04:05"

	start, err := time.Parse(inputFmt, req.StartDateTime)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Invalid start_dt format: %v", req.StartDateTime)
	}
	end, err := time.Parse(inputFmt, req.EndDateTime)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "Invalid end_dt format: %v", req.EndDateTime)
	}

	return &start, &end, nil
}
