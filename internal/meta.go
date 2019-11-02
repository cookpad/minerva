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
	ExpiresAt time.Time `dynamo:"expires_at"`
	PKey      string    `dynamo:"pk"`
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
	if err := x.table.Update("pk", "object:counter").Add("id", 1).Value(&result); err != nil {
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
	if err := x.table.Get("pk", pkey).One(&result); err != nil {
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
		ExpiresAt: now.Add(time.Hour * 24 * 365),
		PKey:      toPartitionKey(partitionKey),
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

/*
func (x *MetaDynamoDB) LockPartition(partitionKey string) (bool, error) {
	now := time.Now().UTC()
	pindex := metaRecord{
		ExpiresAt: now.Add(time.Hour * 24 * 365),
		PKey:      partitionKey,
	}

	if err := x.table.Put(pindex).If("attribute_not_exists(partition_key) OR expires_at < ?", now).Run(); err != nil {
		if isConditionalCheckErr(err) {
			// The partition key already exists
			return true, nil
		}

		return false, errors.Wrapf(err, "Fail to put parition key: %v", pindex)
	}

	return false, nil
}
*/
