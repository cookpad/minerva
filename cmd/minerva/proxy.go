package main

import (
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/m-mizutani/minerva/pkg/api"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

type proxyArguments struct {
	addr string
	port int
}

func proxyCommand(args *arguments) *cli.Command {
	var proxyArgs proxyArguments
	var apiArgs api.MinervaHandler

	return &cli.Command{
		Name:  "proxy",
		Usage: "minerva API proxy server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "addr",
				Aliases:     []string{"a"},
				Value:       "127.0.0.1",
				Usage:       "Bind address",
				Destination: &proxyArgs.addr,
			},
			&cli.IntFlag{
				Name:        "port",
				Aliases:     []string{"p"},
				Value:       10080,
				Usage:       "Bind port number",
				Destination: &proxyArgs.port,
			},
			&cli.StringFlag{
				Name:        "dbname",
				Aliases:     []string{"d"},
				Usage:       "Athena DB name",
				Destination: &apiArgs.DatabaseName,
				EnvVars:     []string{"DB_NAME"},
			},
			&cli.StringFlag{
				Name:        "output",
				Aliases:     []string{"o"},
				Usage:       "Output S3 path such as s3://my-bucket/out",
				Destination: &apiArgs.OutputPath,
				EnvVars:     []string{"S3_OUTPUT"},
			},
			&cli.StringFlag{
				Name:        "region",
				Aliases:     []string{"r"},
				Usage:       "AWS region",
				Destination: &apiArgs.Region,
				EnvVars:     []string{"REGION"},
			},
			&cli.StringFlag{
				Name:        "search-table",
				Aliases:     []string{"s"},
				Usage:       "Search DynamoDB table name",
				Destination: &apiArgs.MetaTableName,
				EnvVars:     []string{"SEARCH_TABLE_NAME"},
			},

			// Optional parameters
			&cli.StringFlag{
				Name:        "index-table",
				Usage:       "Index table name",
				Value:       "indices",
				Destination: &apiArgs.IndexTableName,
			},
			&cli.StringFlag{
				Name:        "message-table",
				Usage:       "message table name",
				Value:       "messages",
				Destination: &apiArgs.MessageTableName,
			},
		},

		Action: func(c *cli.Context) error {
			logger.WithFields(logrus.Fields{
				"args":      args,
				"proxyArgs": proxyArgs,
				"apiArgs":   apiArgs,
			}).Info("Start API server")

			r := gin.Default()
			v1 := r.Group("/api/v1")
			api.SetupRoute(v1, &apiArgs)

			bindAddr := fmt.Sprintf("%s:%d", proxyArgs.addr, proxyArgs.port)
			return r.Run(bindAddr)
		},
	}
}
