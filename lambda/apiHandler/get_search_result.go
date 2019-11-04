package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/sirupsen/logrus"
)

const hardLimitOfQueryResult = 1000 * 1000 // 1,000,000

type logData struct {
	Tag       string      `json:"tag"`
	Timestamp int64       `json:"timestamp"`
	Log       interface{} `json:"log"`
}

type getQueryExecutionMetaData struct {
	Status         string    `json:"status"`
	Total          int64     `json:"total"`
	Offset         int64     `json:"offset"`
	SubmittedTime  time.Time `json:"submitted_time"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
}

type getQueryExecutionResponse struct {
	QueryID  string                    `json:"query_id"`
	Logs     []logData                 `json:"logs"`
	MetaData getQueryExecutionMetaData `json:"metadata"`
}

func extractLogs(r io.ReadCloser, offset, limit int64) ([]logData, int64, error) {
	var logs []logData
	csvReader := csv.NewReader(r)

	var seq int64
	for ; ; seq++ {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, err
		}

		if len(record) != 3 {
			return nil, 0, fmt.Errorf("Invalid CSV row size: %d:%d", seq, len(record))
		}
		if seq == 0 {
			continue // Skip header
		}

		if offset <= seq && seq < offset+limit {
			var values interface{}
			if err := json.Unmarshal([]byte(record[2]), &values); err != nil {
				return nil, 0, err
			}

			ts, err := strconv.ParseInt(record[1], 10, 64)
			if err != nil {
				return nil, 0, err
			}

			logs = append(logs, logData{
				Tag:       record[0],
				Timestamp: ts,
				Log:       values,
			})
		}
	}

	return logs, seq, nil
}

func loadLogs(region, s3path, limit, offset string) ([]logData, *getQueryExecutionMetaData, apiError) {
	var qLimit int64 = 100
	var qOffset int64 = 0

	if limit != "" {
		if v, err := strconv.ParseInt(limit, 10, 64); err == nil {
			qLimit = v
		} else {
			return nil, nil, wrapUserError(err, 400, "Fail to parse 'limit'")
		}
	}

	if offset != "" {
		if v, err := strconv.ParseInt(offset, 10, 64); err == nil {
			qOffset = v
		} else {
			return nil, nil, wrapUserError(err, 400, "Fail to parse 'offset'")
		}
	}

	logger.WithFields(logrus.Fields{
		"region": region,
		"s3path": s3path,
		"limit":  qLimit,
		"offset": qOffset,
	}).Debug("Download s3 object")

	if qLimit > 10000 {
		return nil, nil, newUserError("limit number is too big, must be under 10000", 400)
	}

	ssn := session.Must(session.NewSession(&aws.Config{Region: &region}))
	s3client := s3.New(ssn)

	s3arr := strings.Split(s3path, "/")
	if len(s3arr) < 4 {
		return nil, nil, newSystemError("Invalid format of S3 path: "+s3path, 500)
	}

	output, err := s3client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3arr[2]),
		Key:    aws.String(strings.Join(s3arr[3:], "/")),
	})
	if err != nil {
		return nil, nil, wrapSystemErrorf(err, 500, "Fail to download a result object on S3: %s", s3path)
	}

	logs, total, err := extractLogs(output.Body, qOffset, qLimit)
	if err != nil {
		return nil, nil, wrapSystemErrorf(err, 500, "Fail to extract log data: %s", s3path)
	}

	return logs, &getQueryExecutionMetaData{Total: total, Offset: qOffset}, nil
}

func getSearchResult(args arguments) (*events.APIGatewayProxyResponse, apiError) {
	logger.WithField("args", args).Info("Start getQuery")

	searchID := args.Request.PathParameters["search_id"]
	limit := args.Request.QueryStringParameters["limit"]
	offset := args.Request.QueryStringParameters["offset"]

	resp := getQueryExecutionResponse{
		QueryID: searchID,
	}

	ssn := session.Must(session.NewSession(&aws.Config{Region: &args.Region}))
	athenaClient := athena.New(ssn)

	output, err := athenaClient.GetQueryExecution(&athena.GetQueryExecutionInput{
		QueryExecutionId: &searchID,
	})
	if err != nil {
		return nil, wrapSystemError(err, 500, "Fail GetQueryExecution in getQuery")
	}

	if output.QueryExecution.Status.SubmissionDateTime != nil {
		resp.MetaData.SubmittedTime = *output.QueryExecution.Status.SubmissionDateTime
	}

	var d time.Duration
	if output.QueryExecution.Status.CompletionDateTime != nil {
		d = output.QueryExecution.Status.CompletionDateTime.Sub(resp.MetaData.SubmittedTime)
	} else {
		d = time.Now().UTC().Sub(resp.MetaData.SubmittedTime)
	}
	resp.MetaData.ElapsedSeconds = d.Seconds()

	resp.MetaData.Status = aws.StringValue(output.QueryExecution.Status.State)
	if resp.MetaData.Status == athena.QueryExecutionStateSucceeded {
		s3path := aws.StringValue(output.QueryExecution.ResultConfiguration.OutputLocation)
		logs, meta, err := loadLogs(args.Region, s3path, limit, offset)
		if err != nil {
			return nil, err
		}
		resp.Logs = logs
		resp.MetaData.Total = meta.Total
		resp.MetaData.Offset = meta.Offset
	}
	logger.WithField("output", output).Debug("done")

	return respond(200, &resp), nil
}
