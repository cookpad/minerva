package api

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"
	"github.com/pkg/errors"
)

type searchID string

type searchMetaData struct {
	Status         queryStatus `json:"status"`
	SubmittedTime  time.Time   `json:"submitted_time"`
	ElapsedSeconds float64     `json:"elapsed_seconds"`
	Query          []Query     `json:"query"`
	StartTime      int64       `json:"start_time"`
	EndTime        int64       `json:"end_time"`
	ScannedSize    int64       `json:"scanned_size"`

	outputPath string // S3 output path
}

type searchItem struct {
	PK string `dynamo:"pk"` // Primary Key (Hash key) of DynamoDB
	SK string `dynamo:"sk"` // Secondary Key (Range key) of DynamoDB

	ID            searchID    `dynamo:"id"`
	Status        queryStatus `dynamo:"status"`
	Query         []Query     `dynamo:"query"`
	StartTime     time.Time   `dynamo:"start_time"`
	EndTime       time.Time   `dynamo:"end_time"`
	CreatedAt     *time.Time  `dynamo:"created_at"`
	CompletedAt   *time.Time  `dynamo:"completed_at"`
	AthenaQueryID string      `dynamo:"athena_query_id"`
	RequestID     string      `dynamo:"request_id"`
	OutputPath    string      `dynamo:"output_path"`
	ScannedSize   int64       `dynamo:"scanned_size"`
}

func (x *searchItem) getElapsedSeconds() float64 {
	if x.CreatedAt == nil {
		return 0
	}
	if x.CompletedAt == nil {
		return time.Now().UTC().Sub(*x.CreatedAt).Seconds()
	}

	return x.CompletedAt.Sub(*x.CreatedAt).Seconds()
}

type searchRepository interface {
	put(*searchItem) error
	get(searchID) (*searchItem, error)
}

type searchRepoDynamoDB struct {
	region    string
	tableName string
}

func searchIDtoKey(id searchID) string {
	return "search:" + string(id)
}

func newSearchRepoDynamoDB(region, tableName string) *searchRepoDynamoDB {
	return &searchRepoDynamoDB{
		region:    region,
		tableName: tableName,
	}
}

func (x *searchRepoDynamoDB) put(item *searchItem) error {
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(x.region)})
	table := db.Table(x.tableName)

	item.PK = searchIDtoKey(item.ID)
	item.SK = "@"
	if err := table.Put(item).Run(); err != nil {
		return errors.Wrapf(err, "Fail to put searchItem: %v", *item)
	}

	return nil
}

func (x *searchRepoDynamoDB) get(id searchID) (*searchItem, error) {
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(x.region)})
	table := db.Table(x.tableName)

	pk := searchIDtoKey(id)
	sk := "@"
	var item searchItem
	if err := table.Get("pk", pk).Range("sk", dynamo.Equal, sk).One(&item); err != nil {
		if err == dynamo.ErrNotFound {
			return nil, nil
		}

		return nil, errors.Wrapf(err, "Fail to get searchItem: %s", id)
	}

	return &item, nil
}
