package api

import (
	"fmt"
	"math/rand"
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
	LogTotal   int
	LogLimit   int
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

type logSampleGenerator struct {
	seq int
}

func (x *logSampleGenerator) New() *logData {
	sampleColor := []string{"blue", "orange", "red"}
	x.seq++

	return &logData{
		Tag:       "test.1",
		Timestamp: time.Now().Unix(),
		Log: map[string]string{
			"seq":   fmt.Sprintf("%d", x.seq),
			"steps": fmt.Sprintf("%d", rand.Intn(64)),
			"port":  fmt.Sprintf("%d", rand.Intn(65655)),
			"color": sampleColor[rand.Intn(len(sampleColor))],
		},
	}
}

func (x *MockHandler) GetSearchLogs(c *gin.Context) (*Response, Error) {
	queryID := c.Param("query_id")
	pLimit := c.Query("limit")
	pOffset := c.Query("offset")

	Logger.WithFields(logrus.Fields{
		"args":    x,
		"limit":   pLimit,
		"offset":  pOffset,
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
	resp.MetaData.Status = statusRunning

	var offset int64 = 0
	if pOffset != "" {
		if v, err := strconv.ParseInt(pOffset, 10, 64); err == nil {
			offset = v
		} else {
			return nil, wrapUserError(err, 400, "Fail to parse 'offset'")
		}
	}
	var limit int64 = int64(x.LogLimit)
	if pLimit != "" {
		if v, err := strconv.ParseInt(pLimit, 10, 64); err == nil {
			limit = v
		} else {
			return nil, wrapUserError(err, 400, "Fail to parse 'limit'")
		}
	}
	Logger.Infof("offset=%d, limit=%d", offset, limit)

	if resp.MetaData.ElapsedSeconds >= 3 {
		resp.MetaData.Status = statusSuccess
		gen := logSampleGenerator{}

		var logs []*logData
		for i := 0; i < x.LogTotal; i++ {
			logs = append(logs, gen.New())
		}

		meta := getLogsMetaData{
			Total:  int64(len(logs)),
			Offset: offset,
			Limit:  limit,
		}

		bp := offset
		ep := offset + limit
		if bp > meta.Total {
			bp = meta.Total
		}
		if ep > meta.Total {
			ep = meta.Total
		}

		resp.Logs = logs[bp:ep]
		resp.MetaData.Total = meta.Total
		resp.MetaData.Offset = meta.Offset
		resp.MetaData.Limit = meta.Limit
		Logger.WithField("len(logs)", len(resp.Logs)).WithField("meta", meta).Info("response")
	}

	return &Response{200, &resp}, nil
}

func (x *MockHandler) GetSearchTimeSeries(c *gin.Context) (*Response, Error) {
	return nil, nil
}
