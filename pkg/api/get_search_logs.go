package api

import (
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
	searchMetaData
	Total    int64    `json:"total"`
	SubTotal int64    `json:"sub_total"`
	Offset   int64    `json:"offset"`
	Limit    int64    `json:"limit"`
	Tags     []string `json:"tags"`
}

type GetSearchLogsResponse struct {
	ID       searchID             `json:"search_id"`
	Logs     []*logData           `json:"logs"`
	MetaData GetSearchLogMetaData `json:"metadata"`
}

func (x MinervaHandler) GetSearchLogs(c *gin.Context) (*Response, Error) {
	id := searchID(c.Param("search_id"))

	Logger.WithFields(logrus.Fields{
		"args":     x,
		"searchID": id,
	}).Info("Start getSearchLogs")

	resp := GetSearchLogsResponse{
		ID: id,
	}

	meta, err := x.getMetaData(id)
	if err != nil {
		return nil, err
	}

	resp.MetaData.searchMetaData = *meta

	if resp.MetaData.Status == statusSuccess {
		s3path := meta.outputPath

		logSet, err := loadLogs(x.Region, s3path, c)
		if err != nil {
			return nil, err
		}
		resp.Logs = logSet.Logs
		resp.MetaData.Total = logSet.Total
		resp.MetaData.Offset = logSet.Filter.Offset
		resp.MetaData.Limit = logSet.Filter.Limit
		resp.MetaData.SubTotal = logSet.SubTotal
		resp.MetaData.Tags = logSet.Tags
	}
	Logger.WithField("resp", resp).Debug("Done getSearchLogs")

	return &Response{200, &resp}, nil
}
