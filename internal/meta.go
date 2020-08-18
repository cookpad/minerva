package internal

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/guregu/dynamo"
	"github.com/pkg/errors"
)

type MetaAccessor interface {
	GetObjecID(s3bucket, s3key string) (int64, error)
	HeadPartition(partitionKey string) (bool, error)
	PutPartition(partitionKey string) error
}

type MetaDynamoDB struct {
	table         dynamo.Table
	cacheObjectID map[string]int64
}

type metaRecord struct {
	ExpiresAt int64  `dynamo:"expires_at"`
	PKey      string `dynamo:"pk"`
	SKey      string `dynamo:"sk"`
}

type metaObjectCount struct {
	metaRecord
	ID int64 `dynamo:"id"`
}

// NewMetaDynamoDB is a constructor of MetaDynamoDB as MetaAccessor
func NewMetaDynamoDB(region, tableName string) *MetaDynamoDB {
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(region)})
	table := db.Table(tableName)

	meta := MetaDynamoDB{
		table:         table,
		cacheObjectID: make(map[string]int64),
	}
	return &meta
}

func (x *MetaDynamoDB) GetObjecID(s3bucket, s3key string) (int64, error) {
	s3path := s3bucket + "/" + s3key
	if id, ok := x.cacheObjectID[s3path]; ok {
		return id, nil
	}

	var result metaObjectCount
	var inc int64 = 1
	query := x.table.
		Update("pk", "meta:indexer").
		Range("sk", "counter").
		Add("id", inc)
	if err := query.Value(&result); err != nil {
		return 0, errors.Wrap(err, "Fail to update Object ID in DynamoDB")
	}

	x.cacheObjectID[s3path] = result.ID

	return result.ID, nil
}

func toPartitionKey(partition string) string {
	return "partition:" + partition
}

func (x *MetaDynamoDB) HeadPartition(partitionKey string) (bool, error) {
	var result metaRecord
	pkey := toPartitionKey(partitionKey)
	if err := x.table.Get("pk", pkey).Range("sk", dynamo.Equal, "@").One(&result); err != nil {
		if err == dynamo.ErrNotFound {
			return false, nil
		}

		return false, errors.Wrapf(err, "Fail to get partition key: %s", pkey)
	}

	return true, nil
}

func (x *MetaDynamoDB) PutPartition(partitionKey string) error {
	now := time.Now().UTC()
	pindex := metaRecord{
		ExpiresAt: now.Add(time.Hour * 24 * 365).Unix(),
		PKey:      toPartitionKey(partitionKey),
		SKey:      "@",
	}

	if err := x.table.Put(pindex).Run(); err != nil {
		return errors.Wrapf(err, "Fail to put parition key: %v", pindex)
	}

	return nil
}

func isConditionalCheckErr(err error) bool {
	if aerr, ok := err.(awserr.RequestFailure); ok {
		return aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException
	}
	return false
}

func isResourceNotFoundErr(err error) bool {
	if ae, ok := err.(awserr.RequestFailure); ok {
		return ae.Code() == dynamodb.ErrCodeResourceNotFoundException
	}
	return false
}
