package main

import (
	"fmt"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/m-mizutani/minerva/pkg/api"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var logger = logrus.New()

type parameters struct {
	addr string
	port int
}

func main() {
	logger.SetLevel(logrus.DebugLevel)
	var args api.MinervaHandler
	var params parameters

	api.Logger = logger

	app := &cli.App{
		Name:  "api",
		Usage: "minerva API proxy server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "addr",
				Aliases:     []string{"a"},
				Value:       "127.0.0.1",
				Usage:       "Bind address",
				Destination: &params.addr,
			},
			&cli.IntFlag{
				Name:        "port",
				Aliases:     []string{"p"},
				Value:       10080,
				Usage:       "Bind port number",
				Destination: &params.port,
			},
			&cli.StringFlag{
				Name:        "dbname",
				Aliases:     []string{"d"},
				Usage:       "Athena DB name",
				Destination: &args.DatabaseName,
				EnvVars:     []string{"DB_NAME"},
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Usage:       "Output S3 path such as s3://my-bucket/out",
				Destination: &args.OutputPath,
				EnvVars:     []string{"S3_OUTPUT"},
			},
			&cli.StringFlag{
				Name:        "region",
				Aliases:     []string{"r"},
				Usage:       "AWS region",
				Destination: &args.Region,
				EnvVars:     []string{"REGION"},
			},
			&cli.StringFlag{
				Name:        "search-table",
				Aliases:     []string{"s"},
				Usage:       "Search DynamoDB table name",
				Destination: &args.MetaTableName,
				EnvVars:     []string{"SEARCH_TABLE_NAME"},
			},

			// Optional parameters
			&cli.StringFlag{
				Name:        "index-table",
				Usage:       "Index table name",
				Value:       "indices",
				Destination: &args.IndexTableName,
			},
			&cli.StringFlag{
				Name:        "message-table",
				Usage:       "message table name",
				Value:       "messages",
				Destination: &args.MessageTableName,
			},
		},

		Action: func(c *cli.Context) error {
			logger.WithFields(logrus.Fields{
				"args":   args,
				"params": params,
			}).Info("Start API server")

			r := gin.Default()
			v1 := r.Group("/api/v1")
			api.SetupRoute(v1, &args)

			bindAddr := fmt.Sprintf("%s:%d", params.addr, params.port)
			return r.Run(bindAddr)
		},
	}

	if err := app.Run(os.Args); err != nil {
		logger.WithError(err).Fatal("Server error")
	}
}
