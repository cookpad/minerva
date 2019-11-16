package main

import (
	"time"

	"github.com/m-mizutani/minerva/internal"
)

type arguments struct {
	BaseTime      time.Time
	BaseRegion    string
	S3Region      string
	S3Bucket      string
	S3Prefix      string
	MergeQueueURL string
}

type mergeTarget struct {
	Schema  internal.ParquetSchemaName
	DirName string
}

func listParquet(args arguments) error {
	targetSchemas := []internal.ParquetSchemaName{
		internal.ParquetSchemaIndex,
		internal.ParquetSchemaMessage,
	}

	for _, schema := range targetSchemas {
		root := internal.ParquetLocation{
			Bucket:    args.S3Bucket,
			Region:    args.S3Region,
			Prefix:    args.S3Prefix,
			Timestamp: args.BaseTime,
			Schema:    schema,
		}

		src, dst := root, root
		src.MergeStat = internal.ParquetMergeStatUnmerged
		dst.MergeStat = internal.ParquetMergeStatMerged

		ch := internal.FindS3Objects(args.S3Region, args.S3Bucket, src.PartitionAndMergeStat()+"/")

		if err := sendMergeQueue(ch, dst, args.BaseRegion, args.MergeQueueURL); err != nil {
			return err
		}
	}

	return nil
}
