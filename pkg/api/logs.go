package api

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/itchyny/gojq"
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

func extractLogs(ch chan *logQueue, offset, limit int64) ([]*logData, int64, error) {
	var logs []*logData
	var total int64

	for q := range ch {
		if q.Error != nil {
			return nil, 0, q.Error
		}
		total++

		if offset <= q.Seq && q.Seq < offset+limit {
			log, err := recordToLogData(q.Record)
			if err != nil {
				return nil, 0, err
			}

			logs = append(logs, log)
		}
	}

	return logs, total, nil
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

func loadLogs(region, s3path, limit, offset, filter string) ([]*logData, *getLogsMetaData, Error) {
	var qLimit int64 = 100
	var qOffset int64 = 0

	if limit != "" {
		if v, err := strconv.ParseInt(limit, 10, 64); err == nil {
			qLimit = v
		} else {
			return nil, nil, wrapUserError(err, 400, "Fail to parse 'limit'")
		}
	}

	if offset != "" {
		if v, err := strconv.ParseInt(offset, 10, 64); err == nil {
			qOffset = v
		} else {
			return nil, nil, wrapUserError(err, 400, "Fail to parse 'offset'")
		}
	}

	if filter != ""{
		jq, err := gojq.Parse(filter)
		if err != nil {
			return nil, nil, wrapUserError(err, 400, "Fail to parse filter (invalid jq query)")
		}
		Logger.WithField("jq", jq).Info("Compiled jq")
	}

	Logger.WithFields(logrus.Fields{
		"region": region,
		"s3path": s3path,
		"limit":  qLimit,
		"offset": qOffset,
	}).Debug("Download s3 object")

	if qLimit > 10000 {
		return nil, nil, newUserErrorf(400, "limit number is too big, must be under 10000")
	}

	ch, err := getLogStream(region, s3path)
	if err != nil {
		return nil, nil, wrapSystemErrorf(err, 500, "Fail to get log stream: %s", s3path)
	}

	logs, total, err := extractLogs(ch, qOffset, qLimit)
	if err != nil {
		return nil, nil, wrapSystemErrorf(err, 500, "Fail to extract log data: %s", s3path)
	}

	return logs, &getLogsMetaData{Total: total, Offset: qOffset, Limit: qLimit}, nil
}
