package api

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

type queryMeta struct {
	query  string
	ExecAt time.Time
}

type MockHandler struct {
	mapQueryID map[string]*queryMeta
}

func NewMockHandler() *MockHandler {
	return &MockHandler{
		mapQueryID: make(map[string]*queryMeta),
	}
}

func (x *MockHandler) ExecSearch(c *gin.Context) (*Response, Error) {
	queryID := uuid.New().String()
	x.mapQueryID[queryID] = &queryMeta{
		ExecAt: time.Now(),
	}

	return &Response{201, &ExecSearchResponse{
		QueryID: queryID,
	}}, nil
}

func (x *MockHandler) GetSearchLogs(c *gin.Context) (*Response, Error) {
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

	q, ok := x.mapQueryID[queryID]
	if !ok {
		Logger.Error("invalid query ID")
		return &Response{404, "query not found"}, nil
	}

	now := time.Now()
	resp.MetaData.ElapsedSeconds = float64(now.Sub(q.ExecAt))
	resp.MetaData.Status = "RUNNING"

	var qOffset int64 = 0
	if offset != "" {
		if v, err := strconv.ParseInt(offset, 10, 64); err == nil {
			qOffset = v
		} else {
			return nil, wrapUserError(err, 400, "Fail to parse 'offset'")
		}
	}

	if resp.MetaData.ElapsedSeconds >= 3 {
		logs := []*logData{
			{
				Tag:       "test.1",
				Timestamp: time.Now().Unix(),
				Log: map[string]string{
					"abc": "123",
				},
			},
			{
				Tag:       "test.2",
				Timestamp: time.Now().Unix(),
				Log: map[string]string{
					"deb": "345",
				},
			},
		}

		meta := getLogsMetaData{
			Total:  10,
			Offset: qOffset,
			Limit:  int64(len(logs)),
		}

		resp.Logs = logs
		resp.MetaData.Total = meta.Total
		resp.MetaData.Offset = meta.Offset
		resp.MetaData.Limit = meta.Limit
	}

	return &Response{200, &resp}, nil
}

func (x *MockHandler) GetSearchTimeSeries(c *gin.Context) (*Response, Error) {
	return nil, nil
}
