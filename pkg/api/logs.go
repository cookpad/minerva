package api

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gin-gonic/gin"
	"github.com/itchyny/gojq"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type getLogsMetaData struct {
	Total  int64 `json:"total"`
	Offset int64 `json:"offset"`
	Limit  int64 `json:"limit"`
}

type logQueue struct {
	Seq    int64
	Record []string
	Error  error
}

func recordToLogData(record []string) (*logData, error) {
	var values interface{}
	if err := json.Unmarshal([]byte(record[2]), &values); err != nil {
		return nil, err
	}

	ts, err := strconv.ParseInt(record[1], 10, 64)
	if err != nil {
		return nil, err
	}

	return &logData{
		Tag:       record[0],
		Timestamp: ts,
		Log:       values,
	}, nil
}

type logFilter struct {
	Offset int64
	Limit  int64
	Query  *gojq.Query
	Tags   map[string]bool
	Begin  *int64
	End    *int64
}

type queryString interface {
	Query(string) string
}

func buildLogFilter(qs queryString) (*logFilter, Error) {
	filter := &logFilter{
		Limit:  50,
		Offset: 0,
	}

	if v := qs.Query("limit"); v != "" {
		if d, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.Limit = d
		} else {
			return nil, wrapUserError(err, 400, "Fail to parse 'limit'")
		}
	}

	if v := qs.Query("offset"); v != "" {
		if d, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.Offset = d
		} else {
			return nil, wrapUserError(err, 400, "Fail to parse 'offset'")
		}
	}

	if v := qs.Query("query"); v != "" {
		if q, err := gojq.Parse(v); err == nil {
			filter.Query = q
		} else {
			return nil, wrapUserError(err, 400, "Fail to parse query (invalid jq query)")
		}
	}

	if v := qs.Query("tags"); v != "" {
		filter.Tags = map[string]bool{}

		for _, tag := range strings.Split(v, ",") {
			filter.Tags[tag] = true
		}
	}

	if v := qs.Query("begin"); v != "" {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.Begin = &ts
		} else {
			return nil, wrapUserError(err, 400, "Fail to parse 'begin', must be integer")
		}
	}

	if v := qs.Query("end"); v != "" {
		if ts, err := strconv.ParseInt(v, 10, 64); err == nil {
			filter.End = &ts
		} else {
			return nil, wrapUserError(err, 400, "Fail to parse 'end', must be integer")
		}
	}

	Logger.WithField("filter", filter).Debug("Built filter")
	return filter, nil
}

type logDataSet struct {
	Logs           []*logData
	Tags           []string
	Total          int64
	SubTotal       int64
	FirstTimestamp int64
	LastTimestamp  int64
	Filter         logFilter
}

type tagSet struct {
	tags map[string]struct{}
}

func newTagSet() *tagSet         { return &tagSet{tags: make(map[string]struct{})} }
func (x *tagSet) add(tag string) { x.tags[tag] = struct{}{} }
func (x *tagSet) toList() []string {
	var tagList []string
	for k := range x.tags {
		tagList = append(tagList, k)
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i] < tagList[j] })
	return tagList
}

func extractLogs(ch chan *logQueue, filter logFilter) (*logDataSet, error) {
	var logs []*logData
	var total, seq int64
	tags := newTagSet()

	for q := range ch {
		if q.Error != nil {
			return nil, q.Error
		}
		total++

		log, err := recordToLogData(q.Record)
		if err != nil {
			return nil, err
		}

		// tags has all set of tag in whole log data.
		tags.add(log.Tag)

		if filter.Tags != nil {
			if _, ok := filter.Tags[log.Tag]; !ok {
				continue
			}
		}

		if filter.Begin != nil && log.Timestamp < *filter.Begin {
			continue
		}
		if filter.End != nil && *filter.End < log.Timestamp {
			continue
		}

		if filter.Query != nil {
			iter := filter.Query.Run(log.Log)
			for {
				v, ok := iter.Next()
				if !ok {
					break
				}
				if err, ok := v.(error); ok {
					return nil, err
				}

				if v != nil {
					if filter.Offset <= seq && seq < filter.Offset+filter.Limit {
						// Need to keep map[string]interface{} format for view.
						if reflect.ValueOf(v).Kind() != reflect.Map {
							v = map[string]string{"": fmt.Sprintf("%v", v)}
						}
						logs = append(logs, &logData{Tag: log.Tag, Timestamp: log.Timestamp, Log: v})
					}
					seq++
				}
			}
		} else {
			if filter.Offset <= seq && seq < filter.Offset+filter.Limit {
				logs = append(logs, log)
			}
			seq++
		}
	}

	dataSet := &logDataSet{
		Logs:     logs,
		Total:    total,
		SubTotal: seq,
		Tags:     tags.toList(),
		Filter:   filter,
	}
	Logger.WithField("dataSet", dataSet).Info("retrieved")

	return dataSet, nil
}

func getLogStream(region, s3path string) (chan *logQueue, error) {
	Logger.WithFields(logrus.Fields{
		"region": region,
		"s3path": s3path,
	}).Debug("Download s3 object")

	ssn := session.Must(session.NewSession(&aws.Config{Region: &region}))
	s3client := s3.New(ssn)

	s3arr := strings.Split(s3path, "/")
	if len(s3arr) < 4 {
		return nil, fmt.Errorf("Invalid format of S3 path: %s", s3path)
	}

	output, err := s3client.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3arr[2]),
		Key:    aws.String(strings.Join(s3arr[3:], "/")),
	})
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to download a result object on S3: %s", s3path)
	}

	ch := make(chan *logQueue, 128)
	go func() {
		defer close(ch)
		csvReader := csv.NewReader(output.Body)

		var seq int64
		for ; ; seq++ {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				ch <- &logQueue{Error: err}
				return
			}

			if len(record) != 3 {
				ch <- &logQueue{Error: fmt.Errorf("Invalid CSV row size: %d:%d", seq, len(record))}
				return
			}
			if seq == 0 {
				continue // Skip header
			}

			ch <- &logQueue{Record: record, Seq: seq}
		}
	}()

	return ch, nil
}

func loadLogs(region, s3path string, c *gin.Context) (*logDataSet, Error) {
	filter, apiErr := buildLogFilter(c)
	if apiErr != nil {
		return nil, apiErr
	}

	Logger.WithFields(logrus.Fields{
		"region": region,
		"s3path": s3path,
		"filter": filter,
	}).Debug("Download s3 object")

	if filter.Limit > 10000 {
		return nil, newUserErrorf(400, "limit number is too big, must be under 10000")
	}

	ch, err := getLogStream(region, s3path)
	if err != nil {
		return nil, wrapSystemErrorf(err, 500, "Fail to get log stream: %s", s3path)
	}

	logSet, err := extractLogs(ch, *filter)
	if err != nil {
		return nil, wrapSystemErrorf(err, 500, "Fail to extract log data: %s", s3path)
	}

	return logSet, nil
}
