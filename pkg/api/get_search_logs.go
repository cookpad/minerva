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
	Total    int64    `json:"total"`
	SubTotal int64    `json:"sub_total"`
	Offset   int64    `json:"offset"`
	Limit    int64    `json:"limit"`
	Tags     []string `json:"tags"`
}

type GetSearchLogsResponse struct {
	QueryID  string               `json:"query_id"`
	Logs     []*logData           `json:"logs"`
	MetaData GetSearchLogMetaData `json:"metadata"`
}

func (x MinervaHandler) GetSearchLogs(c *gin.Context) (*Response, Error) {

	queryID := c.Param("query_id")

	Logger.WithFields(logrus.Fields{
		"args":    x,
		"queryID": queryID,
	}).Info("Start getSearchLogs")

	resp := GetSearchLogsResponse{
		QueryID: queryID,
	}

	status, err := getAthenaQueryStatus(x.Region, queryID)
	if err != nil {
		return nil, err
	}

	resp.MetaData.ElapsedSeconds = status.ElapsedTime.Seconds()
	resp.MetaData.Status = toQueryStatus(status.Status)

	if resp.MetaData.Status == athena.QueryExecutionStateSucceeded {
		s3path := status.OutputPath

		logSet, err := loadLogs(x.Region, s3path, c)
		if err != nil {
			return nil, err
		}
		resp.Logs = logSet.Logs
		resp.MetaData.Total = logSet.Total
		resp.MetaData.Offset = logSet.Offset
		resp.MetaData.Limit = logSet.Limit
		resp.MetaData.SubTotal = logSet.SubTotal
		resp.MetaData.Tags = logSet.Tags
	}
	Logger.WithField("resp", resp).Debug("Done getSearchLogs")

	return &Response{200, &resp}, nil
}
