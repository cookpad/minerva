package api

import (
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/gin-gonic/gin"
)

type getQueryTimeSeriesResponse struct {
	QueryID    string              `json:"query_id"`
	MetaData   getQueryLogMetaData `json:"metadata"`
	TimeSeries map[string][]int    `json:"timeseries"`
}

func getSearchTimeSeries(args Arguments, c *gin.Context) (*apiResponse, apiError) {
	Logger.WithField("args", args).Info("Start getSearchLogs")

	queryID := c.Param("query_id")
	tsData := map[string][]int64{}
	var tsUnitNum int64 = 20

	resp := getQueryTimeSeriesResponse{
		QueryID: queryID,
	}

	status, err := getQueryStatus(args.Region, queryID)
	if err != nil {
		return nil, err
	}

	resp.MetaData.ElapsedSeconds = status.ElapsedTime.Seconds()
	resp.MetaData.Status = status.Status

	var tsMax, tsMin *int64
	if resp.MetaData.Status == athena.QueryExecutionStateSucceeded {
		ch, err := getLogStream(args.Region, status.OutputPath)
		if err != nil {
			return nil, wrapSystemError(err, 500, "Fail to create LogStream")
		}

		for q := range ch {
			log, err := recordToLogData(q.Record)
			if err != nil {
				return nil, wrapSystemError(err, 500, "Fail to convert CSV record")
			}

			if tsMax == nil || *tsMax < log.Timestamp {
				tsMax = &log.Timestamp
			}
			if tsMin == nil || *tsMin > log.Timestamp {
				tsMin = &log.Timestamp
			}

			arr, ok := tsData[log.Tag]
			if !ok {
				arr = []int64{}
				tsData[log.Tag] = arr
			}
			arr = append(arr, log.Timestamp)
		}

		if tsMax == nil || tsMin == nil {
			return nil, newUserErrorf(404, "No log data available")
		}

		tsSpan := *tsMax - *tsMin
		tsUnitSize := tsSpan / tsUnitNum

		for tag, arr := range tsData {
			ts := make([]int, tsUnitSize)

			for _, t := range arr {
				idx := (t - *tsMin) / tsSpan
				ts[idx]++
			}

			resp.TimeSeries[tag] = ts
		}

	}
	Logger.WithField("resp", resp).Debug("Done getSearchLogs")

	return &apiResponse{200, &resp}, nil
}
