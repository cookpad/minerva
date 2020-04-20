package main

import (
	"os"

	"github.com/sirupsen/logrus"

	cli "github.com/urfave/cli/v2"
)

var logger = logrus.New()

func main() {
	var args arguments

	app := &cli.App{
		Name:  "minerva",
		Usage: "CLI utility of minerva",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "stack-name",
				Aliases:     []string{"s"},
				Usage:       "StackName of CloudFormation",
				Required:    true,
				Destination: &args.StackName,
			},
			&cli.StringFlag{
				Name:        "region",
				Aliases:     []string{"r"},
				Usage:       "AWS region",
				Required:    true,
				EnvVars:     []string{"AWS_REGION"},
				Destination: &args.Region,
			},
		},
		Commands: []*cli.Command{
			mergeCommand(&args),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logger.WithError(err).Fatal("Abort")
	}
}
