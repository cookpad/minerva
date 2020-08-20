package models

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
	SrcObjects []S3Object        `json:"src_objects"`
	DstObject  S3Object          `json:"dst_object"`
}

// PartitionQueue is arguments of partitioner to add a new partition
type PartitionQueue struct {
	Location  string            `json:"location"`
	TableName string            `json:"table_name"`
	Keys      map[string]string `json:"keys"`
}
