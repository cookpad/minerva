package main

import (
	"time"

	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
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
	targets := []internal.ParquetSchemaName{
		internal.ParquetSchemaIndex,
		internal.ParquetSchemaMessage,
	}

	for _, tgt := range targets {
		root := internal.ParquetLocation{
			Prefix:    args.S3Prefix,
			Schema:    tgt,
			Timestamp: args.BaseTime,
		}

		resp, err := internal.ListS3Objects(args.S3Region, args.S3Bucket, root.PartitionSchemaPrefix()+"/")
		if err != nil {
			return err
		}

		for _, dir := range resp {
			loc, err := internal.ParseS3Key(dir, args.S3Prefix)
			if err != nil {
				return errors.Wrapf(err, "Fail to parse s3 key: %v", dir)
			}

			loc.Timestamp = args.BaseTime
			loc.Bucket = args.S3Bucket
			loc.Region = args.S3Region

			src, dst := *loc, *loc
			src.MergeStat = internal.ParquetMergeStatUnmerged
			dst.MergeStat = internal.ParquetMergeStatMerged

			ch := internal.FindS3Objects(args.S3Region, args.S3Bucket, src.PartitionAndMergeStat()+"/")

			if err := sendMergeQueue(ch, dst, args.BaseRegion, args.MergeQueueURL); err != nil {
				return err
			}
		}
	}

	return nil
}
