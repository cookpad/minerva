package main

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/aws/aws-lambda-go/events"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/merger"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v2"
)

type loadArguments struct {
	filePath string
	memProf  string

	doNotRemoveObjects bool
	doNotRemoveParquet bool
}

func loadCommand(hdlrArgs *handler.Arguments) *cli.Command {
	var args loadArguments

	return &cli.Command{
		Name:  "load",
		Usage: "Load event data (json) from file",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "sqs-event-file",
				Aliases:     []string{"f"},
				Destination: &args.filePath,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "memory-profile",
				Aliases:     []string{"m"},
				Destination: &args.memProf,
			},
			&cli.BoolFlag{
				Name:        "do-not-remove-objects",
				Aliases:     []string{"d"},
				Usage:       "Do not remove source objects after merging",
				Destination: &args.doNotRemoveObjects,
			},
			&cli.BoolFlag{
				Name:        "do-not-remove-parquet",
				Aliases:     []string{"p"},
				Usage:       "Do not remove source objects after merging",
				Destination: &args.doNotRemoveParquet,
			},
		},
		Action: func(c *cli.Context) error {
			if err := loadHandler(hdlrArgs, &args); err != nil {
				return err
			}
			return nil
		},
	}
}

func loadHandler(hdlrArgs *handler.Arguments, args *loadArguments) error {
	handler.SetLogLevel(hdlrArgs.LogLevel)

	raw, err := ioutil.ReadFile(args.filePath)
	if err != nil {
		return errors.Wrap(err, "Failed to read event file")
	}

	var event events.SQSEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return errors.Wrap(err, "Failed to parse SQS event file")
	}

	for _, record := range event.Records {
		var q models.MergeQueue
		if err := json.Unmarshal([]byte(record.Body), &q); err != nil {
			return errors.Wrap(err, "Failed to parse JSON event file")
		}

		logger.WithField("queue", q).Info("Start merging")
		opt := &merger.MergeOptions{
			DoNotRemoveSrc:     args.doNotRemoveObjects,
			DoNotRemoveParquet: args.doNotRemoveParquet,
		}
		if err := merger.MergeChunk(*hdlrArgs, &q, opt); err != nil {
			return err
		}
	}

	if args.memProf != "" {
		f, err := os.Create(args.memProf)
		if err != nil {
			return errors.Wrap(err, "Failed to create memory profile")
		}
		defer f.Close()
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			return errors.Wrap(err, "Failed to write memory profile")
		}
	}

	return nil
}
