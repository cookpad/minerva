package api

import (
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

const hardLimitOfQueryResult = 1000 * 1000 // 1,000,000

type logData struct {
	Tag       string      `json:"tag"`
	Timestamp int64       `json:"timestamp"`
	Log       interface{} `json:"log"`
}

type GetSearchLogMetaData struct {
	getQueryExecutionMetaData
	Total  int64 `json:"total"`
	Offset int64 `json:"offset"`
	Limit  int64 `json:"limit"`
}

type GetSearchLogsResponse struct {
	QueryID  string               `json:"query_id"`
	Logs     []*logData           `json:"logs"`
	MetaData GetSearchLogMetaData `json:"metadata"`
}

func (x MinervaHandler) GetSearchLogs(c *gin.Context) (*Response, Error) {

	queryID := c.Param("query_id")
	limit := c.Query("limit")
	offset := c.Query("offset")

	Logger.WithFields(logrus.Fields{
		"args":    x,
		"limit":   limit,
		"offset":  offset,
		"queryID": queryID,
	}).Info("Start getSearchLogs")

	resp := GetSearchLogsResponse{
		QueryID: queryID,
	}

	status, err := getQueryStatus(x.Region, queryID)
	if err != nil {
		return nil, err
	}

	resp.MetaData.ElapsedSeconds = status.ElapsedTime.Seconds()
	resp.MetaData.Status = status.Status

	if resp.MetaData.Status == athena.QueryExecutionStateSucceeded {
		s3path := status.OutputPath
		logs, meta, err := loadLogs(x.Region, s3path, limit, offset)
		if err != nil {
			return nil, err
		}
		resp.Logs = logs
		resp.MetaData.Total = meta.Total
		resp.MetaData.Offset = meta.Offset
		resp.MetaData.Limit = meta.Limit
	}
	Logger.WithField("resp", resp).Debug("Done getSearchLogs")

	return &Response{200, &resp}, nil
}
