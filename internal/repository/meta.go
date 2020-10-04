package repository

import (
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/guregu/dynamo"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

// MetaRepository is interface of object repository
type MetaRepository interface {
	GetObjecID(s3path string) (int64, error)
	PutRecordObjects(objects []*MetaRecordObject) error
	GetRecordObjects(recordIDs []string, schema models.ParquetSchemaName) ([]*MetaRecordObject, error)
	HeadPartition(partitionKey string) (bool, error)
	PutPartition(partitionKey string) error
}

// MetaDynamoDB is implementation of MetaRepository
type MetaDynamoDB struct {
	table dynamo.Table
}

type metaBase struct {
	ExpiresAt int64  `dynamo:"expires_at"`
	PKey      string `dynamo:"pk"`
	SKey      string `dynamo:"sk"`
}

type metaObjectCount struct {
	metaBase
	ID int64 `dynamo:"id"`
}

type MetaRecordObject struct {
	metaBase
	models.S3Object

	RecordID string                   `dynamo:"record_id"`
	Schema   models.ParquetSchemaName `dynamo:"schema"`
}

func (x *MetaRecordObject) HashKey() interface{} {
	return fmt.Sprintf("record/%s", x.RecordID)
}

func (x *MetaRecordObject) RangeKey() interface{} {
	return string(x.Schema)
}

// NewMetaDynamoDB is a constructor of MetaDynamoDB as MetaAccessor
func NewMetaDynamoDB(region, tableName string) MetaRepository {
	db := dynamo.New(session.New(), &aws.Config{Region: aws.String(region)})
	table := db.Table(tableName)

	meta := MetaDynamoDB{
		table: table,
	}
	return &meta
}

func (x *MetaDynamoDB) GetObjecID(s3path string) (int64, error) {
	var result metaObjectCount
	var inc int64 = 1
	query := x.table.
		Update("pk", "meta:indexer").
		Range("sk", "counter").
		Add("id", inc)
	if err := query.Value(&result); err != nil {
		return 0, errors.Wrap(err, "Fail to update Object ID in DynamoDB")
	}
	return result.ID, nil
}

// PutRecordObjects puts set of S3 path of record file to DynamoDB
func (x *MetaDynamoDB) PutRecordObjects(records []*MetaRecordObject) error {
	var items []interface{}
	for _, item := range records {
		item.PKey = item.HashKey().(string)
		item.SKey = item.RangeKey().(string)
		items = append(items, item)
	}

	query := x.table.Batch("pk", "sk").Write().Put(items...)

	if n, err := query.Run(); err != nil {
		return errors.Wrap(err, "Failed to put S3 object path")
	} else if n != len(items) {
		return errors.Wrap(err, "Failed to write all path set")
	}

	return nil
}

// GetRecordObjects retrieves S3 path of record file from DynamoDB
func (x *MetaDynamoDB) GetRecordObjects(recordIDs []string, schema models.ParquetSchemaName) ([]*MetaRecordObject, error) {
	var results []*MetaRecordObject
	var keys []dynamo.Keyed

	for _, id := range recordIDs {
		keys = append(keys, &MetaRecordObject{RecordID: id, Schema: schema})
	}

	if err := x.table.Batch("pk", "sk").Get(keys...).All(&results); err != nil {
		if err == dynamo.ErrNotFound {
			return nil, err
		}
		return nil, errors.Wrap(err, "Failed to batch get S3 object path")
	}

	return results, nil
}

func toPartitionKey(partition string) string {
	return "partition:" + partition
}

func (x *MetaDynamoDB) HeadPartition(partitionKey string) (bool, error) {
	var result metaBase
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
	pindex := metaBase{
		ExpiresAt: now.Add(time.Hour * 24 * 365).Unix(),
		PKey:      toPartitionKey(partitionKey),
		SKey:      "@",
	}

	if err := x.table.Put(pindex).Run(); err != nil {
		return errors.Wrapf(err, "Fail to put parition key: %v", pindex)
	}

	return nil
}
