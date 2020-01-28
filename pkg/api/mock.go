package api

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
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

type actionLog struct {
	Name   string `json:"name"`
	Target string `json:"target"`
}

func mustToJSON(v interface{}) string {
	raw, err := json.Marshal(v)
	if err != nil {
		Logger.Fatal("Fail to marshal JSON:", err)
	}
	return string(raw)
}

func newLogStream(max int) chan *logQueue {
	logs := [][]string{
		[]string{"test.user", "1580000000", mustToJSON(map[string]interface{}{
			"name":  "Ao",
			"color": "blue",
			"rank":  0,
		})},
		[]string{"test.user", "1580000002", mustToJSON(map[string]interface{}{
			"name":  "Tou",
			"color": "orange",
			"rank":  1,
		})},
		[]string{"test.user", "1580000004", mustToJSON(map[string]interface{}{
			"name": "Alice",
			"rank": 100,
		})},
		[]string{"test.user", "1580000005", mustToJSON(map[string]interface{}{
			"name": "Barth",
			"rank": 1,
		})},
		[]string{"test.user", "1580000009", mustToJSON(map[string]interface{}{
			"name": "Chris",
			"rank": 200,
		})},
		[]string{"test.user", "1580000010", mustToJSON(map[string]interface{}{
			"name": "Dymos",
			"rank": 300,
		})},
		[]string{"test.action", "1580000012", mustToJSON(actionLog{
			Name:   "attack",
			Target: "rock",
		})},
		[]string{"test.action", "1580000012", mustToJSON(actionLog{
			Name:   "Ao",
			Target: "paper",
		})},
	}

	ch := make(chan *logQueue, 1)
	go func() {
		defer close(ch)
		for i := 0; i < max; i++ {
			ch <- &logQueue{
				Seq:    int64(i),
				Record: logs[rand.Intn(len(logs))],
			}
		}
	}()
	return ch
}

func (x *MockHandler) GetSearchLogs(c *gin.Context) (*Response, Error) {
	queryID := c.Param("query_id")

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

	if resp.MetaData.ElapsedSeconds >= 3 {
		resp.MetaData.Status = statusSuccess
		filter, apiErr := buildLogFilter(c)
		if apiErr != nil {
			return nil, apiErr
		}
		Logger.WithField("filter", filter).Debug("Built filter")

		logSet, err := extractLogs(newLogStream(x.LogTotal), *filter)
		if err != nil {
			return nil, wrapSystemError(err, 500, "Fail to load logs")
		}

		resp.Logs = logSet.Logs
		resp.MetaData.Total = logSet.Total
		resp.MetaData.Offset = filter.Offset
		resp.MetaData.Limit = filter.Limit
		Logger.WithField("len(logs)", len(resp.Logs)).WithField("meta", resp.MetaData).Info("response")
	}

	return &Response{200, &resp}, nil
}

func (x *MockHandler) GetSearchTimeSeries(c *gin.Context) (*Response, Error) {
	return nil, nil
}
