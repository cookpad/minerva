package models

import (
	"time"
)

// ComposeQueue is sent by indexer and received by composer
type ComposeQueue struct {
	S3Object  S3Object `json:"s3_object"`
	Size      int64    `json:"size"`
	Schema    string   `json:"schema"`
	Partition string   `json:"partition"`
}

// MergeQueue specify src object locations to be merged and destination object location.
type MergeQueue struct {
	Schema     ParquetSchemaName `json:"schema"`
	TotalSize  int64             `json:"total_size"`
	SrcObjects []*S3Object       `json:"src_objects"`
	DstObject  S3Object          `json:"dst_object"`
}

// PartitionQueue is arguments of partitioner to add a new partition
type PartitionQueue struct {
	Location  string            `json:"location"`
	TableName string            `json:"table_name"`
	Keys      map[string]string `json:"keys"`
}

// LogQueue is used in indexer
type LogQueue struct {
	Err       error
	Timestamp time.Time
	Tag       string
	Message   string
	Value     interface{}
	Seq       int32
	Src       S3Object
}

// RecordQueue is used for RecordService.Load
type RecordQueue struct {
	Err     error
	Records []Record
}
