package main

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/merger"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

var logger = internal.Logger

func main() {
	handler.StartLambda(mergeHandler)
}

func mergeHandler(args handler.Arguments) error {
	// Clean up /tmp for undeleted .parquet files
	if _, ok := os.LookupEnv("AWS_LAMBDA_FUNCTION_NAME"); ok {
		tmpDir := "/tmp"
		files, err := ioutil.ReadDir(tmpDir)
		if err != nil {
			return errors.Wrap(err, "Failed to list /tmp")
		}

		for _, file := range files {
			if strings.HasSuffix(file.Name(), ".parquet") {
				if err := os.Remove(filepath.Join(tmpDir, file.Name())); err != nil {
					logger.WithError(err).Warn("can not remove existing .parquet file")
				}
			}
		}
	}

	records, err := args.DecapSQSEvent()
	if err != nil {
		return err
	}

	for _, record := range records {
		var q models.MergeQueue
		if err := record.Bind(&q); err != nil {
			return err
		}

		if err := merger.MergeChunk(args, &q, nil); err != nil {
			return errors.Wrap(err, "Failed composeChunk")
		}
	}

	return nil
}
