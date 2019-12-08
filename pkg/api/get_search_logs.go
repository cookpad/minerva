package api

import (
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/gin-gonic/gin"
)

const hardLimitOfQueryResult = 1000 * 1000 // 1,000,000

type logData struct {
	Tag       string      `json:"tag"`
	Timestamp int64       `json:"timestamp"`
	Log       interface{} `json:"log"`
}

type getQueryLogMetaData struct {
	getQueryExecutionMetaData
	Total  int64 `json:"total"`
	Offset int64 `json:"offset"`
	Limit  int64 `json:"limit"`
}

type getQueryLogsResponse struct {
	QueryID  string              `json:"query_id"`
	Logs     []*logData          `json:"logs"`
	MetaData getQueryLogMetaData `json:"metadata"`
}

func getSearchLogs(args Arguments, c *gin.Context) (*apiResponse, apiError) {
	Logger.WithField("args", args).Info("Start getSearchLogs")

	queryID := c.Param("query_id")
	limit := c.Param("limit")
	offset := c.Param("offset")

	resp := getQueryLogsResponse{
		QueryID: queryID,
	}

	status, err := getQueryStatus(args.Region, queryID)
	if err != nil {
		return nil, err
	}

	resp.MetaData.ElapsedSeconds = status.ElapsedTime.Seconds()
	resp.MetaData.Status = status.Status

	if resp.MetaData.Status == athena.QueryExecutionStateSucceeded {
		s3path := status.OutputPath
		logs, meta, err := loadLogs(args.Region, s3path, limit, offset)
		if err != nil {
			return nil, err
		}
		resp.Logs = logs
		resp.MetaData.Total = meta.Total
		resp.MetaData.Offset = meta.Offset
		resp.MetaData.Limit = meta.Limit
	}
	Logger.WithField("resp", resp).Debug("Done getSearchLogs")

	return &apiResponse{200, &resp}, nil
}
