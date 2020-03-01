package api

import (
	"time"

	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/gin-gonic/gin"
)

type getQueryTimeSeriesResponse struct {
	ID         searchID             `json:"search_id"`
	MetaData   GetSearchLogMetaData `json:"metadata"`
	TimeSeries map[string][]int64   `json:"timeseries"`
	Labels     []string             `json:"labels"`
}

func (x *MinervaHandler) GetSearchTimeSeries(c *gin.Context) (*Response, Error) {
	Logger.WithField("args", x).Info("Start getSearchLogs")

	id := searchID(c.Param("search_id"))
	tsData := map[string][]int64{}
	var tsUnitSize int64 = 20

	resp := getQueryTimeSeriesResponse{
		ID: id,
	}

	meta, err := x.getMetaData(id)
	if err != nil {
		return nil, err
	}

	resp.MetaData.searchMetaData = *meta

	tsMin, tsMax := meta.StartTime, meta.EndTime
	tsSpan := tsMax - tsMin
	tsUnitSpan := float64(tsSpan) / float64(tsUnitSize)

	var labelFmt string
	switch {
	case tsSpan < 24*3600:
		labelFmt = "15:04"
	case tsSpan < 365*24*3600:
		labelFmt = "Jan-06 15:04"
	default:
		labelFmt = "2006-01-02 15:04"
	}

	for i := int64(0); i < tsUnitSize; i++ {
		fwd := tsUnitSpan * float64(i)
		t := time.Unix(tsMin+int64(fwd), 0)
		resp.Labels = append(resp.Labels, t.Format(labelFmt))
	}

	if resp.MetaData.Status == athena.QueryExecutionStateSucceeded {
		ch, err := getLogStream(x.Region, meta.outputPath)
		if err != nil {
			return nil, wrapSystemError(err, 500, "Fail to create LogStream")
		}

		for q := range ch {
			log, err := recordToLogData(q.Record)
			if err != nil {
				return nil, wrapSystemError(err, 500, "Fail to convert CSV record")
			}

			arr, ok := tsData[log.Tag]
			if !ok {
				arr = make([]int64, tsUnitSize)
				tsData[log.Tag] = arr
			}

			idx := int(float64(log.Timestamp-tsMin) / tsUnitSpan)
			if idx >= len(arr) {
				idx = len(arr) - 1
			}
			arr[idx]++
		}
	}

	resp.TimeSeries = tsData

	Logger.WithField("resp", resp).Debug("Done getSearchLogs")

	return &Response{200, &resp}, nil
}
