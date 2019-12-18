package indexer

import (
	"os"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

const (
	indexQueueSize = 128
)

type arguments struct {
	IndexTable     string
	MessageTable   string
	MetaTable      string
	ObjectQueue    string
	PartitionQueue string
	BaseRegion     string
	Src            s3Loc
	Dst            s3Loc
	Reader         *rlogs.Reader
}

var profiler = internal.NewProfile()

func makeIndex(args arguments) error {
	ch := LoadMessage(args.Src, args.Reader)
	meta := internal.NewMetaDynamoDB(args.BaseRegion, args.MetaTable)
	dumpers, err := DumpParquetFiles(ch, meta)

	logger.WithFields(logrus.Fields{
		"dumpers": dumpers,
	}).Debug("Done dump parquet file(s)")

	if err != nil {
		return errors.Wrap(err, "Fail to dump parquet")
	}

	for _, dumper := range dumpers {

		for _, f := range dumper.Files() {
			dst := f.dst
			dst.Bucket = args.Dst.Bucket
			dst.Prefix = args.Dst.Prefix

			if err := internal.UploadFileToS3(f.filePath, args.Dst.Region, dst.Bucket, dst.S3Key()); err != nil {
				return errors.Wrapf(err, "Fail to emit file: %v", dst)
			}

			if err := os.Remove(f.filePath); err != nil {
				return errors.Wrapf(err, "Fail to remove dump file: %s", f.filePath)
			}

			partQueue := internal.PartitionQueue{
				Location:  dst.PartitionLocation(),
				TableName: dst.TableName(),
				Keys:      dst.PartitionKeys(),
			}
			logger.WithField("q", partQueue).Info("Partition queue")
			if err := internal.SendSQS(&partQueue, args.BaseRegion, args.PartitionQueue); err != nil {
				return errors.Wrap(err, "Fail to send parition queue")
			}
		}
	}

	logger.WithField("profile", profiler.Pack()).Info("Done indexing")

	return nil
}
