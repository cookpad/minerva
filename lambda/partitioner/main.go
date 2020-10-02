package main

import (
	"fmt"

	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

var logger = handler.Logger

func main() {
	handler.StartLambda(Handler)
}

// Handler is exported for testing
func Handler(args handler.Arguments) error {
	records, err := args.DecapSQSEvent()
	if err != nil {
		return err
	}

	meta := args.MetaService()
	outputPath := fmt.Sprintf("s3://%s/%soutput", args.S3Bucket, args.S3Prefix)
	for _, record := range records {
		var q models.PartitionQueue
		if err := record.Bind(&q); err != nil {
			return err
		}

		logger.WithField("queue", q).Info("Run composer")

		if err := createPartition(args.AwsRegion, args.AthenaDBName, &q, meta, outputPath); err != nil {
			return errors.Wrapf(err, "Fail to create partition")
		}
	}

	return nil
}
