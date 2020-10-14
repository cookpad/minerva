package main

import (
	"os"

	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/urfave/cli/v2"
)

var logger = handler.Logger

func configure(args *handler.Arguments) {
	handler.SetLogLevel(args.LogLevel)
}

func main() {
	args := &handler.Arguments{
		NewS3:  adaptor.NewS3Client,
		NewSQS: adaptor.NewSQSClient,
	}
	if err := args.BindEnvVars(); err != nil {
		logger.WithError(err).Fatal("Failed import env vars")
	}

	app := &cli.App{
		Name:  "merger",
		Usage: "Minerva merger",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "sentry-dsn",
				EnvVars:     []string{"SENTRY_DSN"},
				Destination: &args.SentryDSN,
			},
			&cli.StringFlag{
				Name:        "sentry-env",
				EnvVars:     []string{"SENTRY_ENVIRONMENT"},
				Destination: &args.SentryEnv,
			},
			&cli.StringFlag{
				Name:        "log-level",
				Aliases:     []string{"l"},
				EnvVars:     []string{"LOG_LEVEL"},
				Destination: &args.LogLevel,
			},
		},

		Commands: []*cli.Command{
			loopCommand(args),
			oneshotCommand(args),
			loadCommand(args),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logger.Fatal(err)
	}
}
