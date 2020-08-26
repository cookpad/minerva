package models

import (
	"crypto/sha1"
	"fmt"
	"log"
	"strings"
	"time"
)

// ParquetSchemaName identifies schema name
type ParquetSchemaName string

const (
	// ParquetSchemaIndex indicates IndexRecord parquet schema
	ParquetSchemaIndex ParquetSchemaName = "index"
	// ParquetSchemaMessage indicates MessageRecord
	ParquetSchemaMessage ParquetSchemaName = "message"
)

// AthenaTableName idicates table name and directory name
type AthenaTableName string

const (
	// AthenaTableIndex is table name for index objects and directory name
	AthenaTableIndex AthenaTableName = "indices"
	// AthenaTableMessage is table name for message objects and directory name
	AthenaTableMessage = "messages"
)

// ParquetMergeStat indicates path of merged/unmerged objects
type ParquetMergeStat string

const (
	// ParquetMergeStatMerged indicates merged object path
	ParquetMergeStatMerged ParquetMergeStat = "merged"
	// ParquetMergeStatUnmerged indicates unmerged object path
	ParquetMergeStatUnmerged ParquetMergeStat = "unmerged"
)

const (
	s3DirNameIndex    = "indices"
	s3DirNameMessage  = "messages"
	s3DirNameMerged   = "merged"
	s3DirNameUnmerged = "unmerged"
)

var schemaDirNameMap = map[string]string{
	"index":   s3DirNameIndex,
	"message": s3DirNameMessage,
}

// BuildMergedS3ObjectKey creates S3 key of merged object from paramters
func BuildMergedS3ObjectKey(prefix, schema, partition, chunkKey string) string {
	schemaDirName, ok := schemaDirNameMap[schema]
	if !ok {
		log.Fatalf("Invalid schema name: %s", schema)
	}
	return prefix + strings.Join([]string{
		schemaDirName,
		partition,
		fmt.Sprintf("merged-%s.parquet", chunkKey),
	}, "/")
}

// ParquetLocation indicates S3 path of parquet file. Minerva defines a path rule
// for parquet files on S3 and ParquetLocation includes logics of the rule.
//
// Key Format:
// s3://{bucket}/{prefix}{schema}/{partition}/{merged,unmerged}/{hour}/{srcBucket}/{srcKey}.parquet
type ParquetLocation struct {
	Region       string
	Bucket       string
	Prefix       string
	MergeStat    ParquetMergeStat
	Schema       ParquetSchemaName
	Timestamp    time.Time
	SrcBucket    string
	SrcKey       string
	FileNameSalt string
}

// S3Key returns full S3 key of the parquet object on S3.
func (x ParquetLocation) S3Key() string {
	var key string
	switch x.MergeStat {
	case ParquetMergeStatMerged:
		log.Fatal("ParquetLocation.S3Key for merged object is no longer used")
	case ParquetMergeStatUnmerged:
		key = x.Prefix + strings.Join([]string{
			"raw",
			x.schemaName(),
			x.Partition(),
		}, "/")
	}

	if x.SrcBucket != "" {
		key += "/" + x.SrcBucket
	}

	key += "/" + x.SrcKey
	if !strings.HasSuffix(key, ".parquet") {
		// Avoid file name conflict.
		if x.FileNameSalt != "" {
			v := x.SrcKey + x.FileNameSalt
			h := sha1.New()
			h.Write([]byte(v))
			key += fmt.Sprintf(".%x", h.Sum(nil))
		}
		key += ".parquet"
	}

	return key
}

func (x ParquetLocation) schemaName() string {
	switch x.Schema {
	case ParquetSchemaIndex:
		return s3DirNameIndex
	case ParquetSchemaMessage:
		return s3DirNameMessage
	default:
		log.Fatalf("Invalid schema: %v", x.Schema)
		return ""
	}
}

// TableName retruns Athena table name according to schema
func (x ParquetLocation) TableName() string {
	return x.schemaName()
}

// PartitionPrefix returns a part of partition path. Main purpose is to manage
// dumper
func (x ParquetLocation) PartitionPrefix() string {
	return x.Prefix + strings.Join([]string{
		x.schemaName(),
		x.Partition(),
	}, "/")
}

// PartitionAndMergeStat returns a part of partition path. Main purpose is to manage
// dumper
func (x ParquetLocation) PartitionAndMergeStat() string {
	return x.Prefix + strings.Join([]string{
		x.schemaName(),
		x.Partition(),
		string(x.MergeStat),
	}, "/")
}

// PartitionSchemaPrefix returns a part of partition path including only schema.
// The function is for ListObjects to list tags
func (x ParquetLocation) PartitionSchemaPrefix() string {
	return x.Prefix + strings.Join([]string{
		x.schemaName(),
	}, "/")
}

// PartitionLocation returns partition key part of S3 location.
// The function is for creating partition by ALTER TABLE.
func (x ParquetLocation) PartitionLocation() string {
	return "s3://" + x.Bucket + "/" + x.PartitionPrefix() + "/"
}

// PartitionKeys returns map of key set for Athena partitioning.
func (x ParquetLocation) PartitionKeys() map[string]string {
	return map[string]string{
		"dt": x.DtKey(),
	}
}

// DtKey returns date key for "dt="
func (x ParquetLocation) DtKey() string {
	return x.Timestamp.Format("2006-01-02-15")
}

func (x ParquetLocation) partitionDate() string {
	return "dt=" + x.DtKey()
}

// Partition returns a partition related part of S3 key.
func (x ParquetLocation) Partition() string {
	return x.partitionDate()
}
