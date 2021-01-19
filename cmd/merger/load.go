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
	filePath   string
	recordPath string
	memProf    string

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
				Required:    false,
			},
			&cli.StringFlag{
				Name:        "record-file",
				Aliases:     []string{"r"},
				Destination: &args.recordPath,
				Required:    false,
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
			&cli.StringFlag{
				Name:        "meta-table-name",
				Aliases:     []string{"t"},
				Destination: &hdlrArgs.MetaTableName,
				Required:    true,
			},
			&cli.StringFlag{
				Name:        "aws-region",
				Aliases:     []string{"a"},
				Destination: &hdlrArgs.AwsRegion,
				Required:    true,
			},
		},
		Action: func(c *cli.Context) error {
			configure(hdlrArgs)

			if args.filePath != "" {
				if err := handleSQSMessage(hdlrArgs, &args); err != nil {
					return err
				}
			} else if args.recordPath != "" {
				if err := handleRawMessage(hdlrArgs, &args); err != nil {
					return err
				}
			} else {
				return errors.New("Either one of -f and -r is required")
			}
			return nil
		},
	}
}

func handleSQSMessage(hdlrArgs *handler.Arguments, args *loadArguments) error {
	handler.SetLogLevel(hdlrArgs.LogLevel)

	raw, err := ioutil.ReadFile(args.filePath)
	if err != nil {
		return errors.Wrap(err, "Failed to read event file")
	}

	var event events.SQSEvent
	if err := json.Unmarshal(raw, &event); err != nil {
		return errors.Wrap(err, "Failed to parse SQS event file")
	}
	logger.WithField("event", event).Info("Loaded event")

	for _, record := range event.Records {
		if err := handleRecord(hdlrArgs, args, []byte(record.Body)); err != nil {
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

func handleRawMessage(hdlrArgs *handler.Arguments, args *loadArguments) error {
	raw, err := ioutil.ReadFile(args.recordPath)
	if err != nil {
		return errors.Wrap(err, "Failed to read event file")
	}

	if err := handleRecord(hdlrArgs, args, []byte(raw)); err != nil {
		return err
	}

	return nil
}

func handleRecord(hdlrArgs *handler.Arguments, args *loadArguments, body []byte) error {
	var q models.MergeQueue
	if err := json.Unmarshal(body, &q); err != nil {
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

	return nil
}
