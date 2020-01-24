package api

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

type getQueryExecutionMetaData struct {
	Status         string    `json:"status"`
	SubmittedTime  time.Time `json:"submitted_time"`
	ElapsedSeconds float64   `json:"elapsed_seconds"`
}

type queryStatus struct {
	Status      string
	ElapsedTime time.Duration
	OutputPath  string
}

func getQueryStatus(region, queryID string) (*queryStatus, apiError) {
	ssn := session.Must(session.NewSession(&aws.Config{Region: &region}))
	athenaClient := athena.New(ssn)

	output, err := athenaClient.GetQueryExecution(&athena.GetQueryExecutionInput{
		QueryExecutionId: &queryID,
	})
	if err != nil {
		return nil, wrapSystemError(err, 500, "Fail GetQueryExecution in getQuery")
	}

	status := queryStatus{}
	if output != nil && output.QueryExecution != nil {
		if output.QueryExecution.Status != nil {
			if s := output.QueryExecution.Status.SubmissionDateTime; s != nil {
				if output.QueryExecution.Status.CompletionDateTime != nil {
					status.ElapsedTime = output.QueryExecution.Status.CompletionDateTime.Sub(*s)
				} else {
					status.ElapsedTime = time.Now().UTC().Sub(*s)
				}
			}

			status.Status = aws.StringValue(output.QueryExecution.Status.State)
		}

		if output.QueryExecution.ResultConfiguration != nil {
			status.OutputPath = aws.StringValue(output.QueryExecution.ResultConfiguration.OutputLocation)
		}
	}

	return &status, nil
}
