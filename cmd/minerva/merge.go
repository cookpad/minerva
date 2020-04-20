package main

import (
	"encoding/json"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/lambda"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	cli "github.com/urfave/cli/v2"
)

type mergeArguments struct {
	Begin string
	End   string

	beginTime time.Time
	endTime   time.Time
}

type listParquetEvent struct {
	BaseTime *time.Time `json:"base_time"`
}

func mergeAction(args arguments, mergeArgs mergeArguments) error {
	if err := parseArguments(&mergeArgs); err != nil {
		return err
	}

	resources, err := args.describeStack()
	if err != nil {
		return err
	}

	listObject := resources["listIndexObject"]

	ssn := session.New(&aws.Config{Region: aws.String(args.Region)})
	client := lambda.New(ssn)

	for t := mergeArgs.beginTime; mergeArgs.endTime.After(t); t = t.Add(time.Hour) {
		event := listParquetEvent{
			BaseTime: &t,
		}
		raw, err := json.Marshal(event)
		if err != nil {
			return errors.Wrapf(err, "Fail to marshal listIndexObject event: %v", event)
		}

		input := &lambda.InvokeInput{
			FunctionName: listObject.PhysicalResourceId,
			Payload:      raw,
		}
		logger.WithFields(logrus.Fields{
			"function": *listObject.PhysicalResourceId,
			"payload":  string(raw),
		}).Info("Invoke listIndexObject")

		output, err := client.Invoke(input)
		if err != nil {
			return errors.Wrapf(err, "Fail to invoke listIndexObject: %v", *input)
		}

		logger.WithFields(logrus.Fields{
			"response":   string(output.Payload),
			"statusCode": aws.Int64Value(output.StatusCode),
		}).Info("Done listIndexObject")
	}

	return nil
}

func mergeCommand(args *arguments) *cli.Command {
	var mergeArgs mergeArguments

	return &cli.Command{
		Name:  "merge",
		Usage: "Invoke merge process",
		Action: func(c *cli.Context) error {
			return mergeAction(*args, mergeArgs)
		},
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "begin",
				Aliases:     []string{"b"},
				Usage:       "Begin time of merge (format: 2006-01-02T15:04:05)",
				Required:    true,
				Destination: &mergeArgs.Begin,
			},
			&cli.StringFlag{
				Name:        "end",
				Aliases:     []string{"e"},
				Usage:       "End time of merge (format: 2006-01-02T15:04:05)",
				Required:    true,
				Destination: &mergeArgs.End,
			},
		},
	}
}

func parseArguments(args *mergeArguments) error {
	layout := "2006-01-02T15:04:05"
	var err error

	args.beginTime, err = time.Parse(layout, args.Begin)
	if err != nil {
		return errors.Wrapf(err, "Fail to parse begin time: %v", args.Begin)
	}

	args.endTime, err = time.Parse(layout, args.End)
	if err != nil {
		return errors.Wrapf(err, "Fail to parse end time: %v", args.End)
	}

	return nil
}
