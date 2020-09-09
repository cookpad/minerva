package models

import (
	"fmt"
	"log"
	"strings"
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
