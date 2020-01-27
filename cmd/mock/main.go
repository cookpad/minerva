package main

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/m-mizutani/minerva/pkg/api"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli/v2"
)

var logger = api.Logger

type parameters struct {
	addr string
	port int
}

func main() {
	logger.SetLevel(logrus.DebugLevel)
	var params parameters
	handler := api.NewMockHandler()

	rand.Seed(time.Now().Unix())

	api.Logger = logger

	app := &cli.App{
		Name:  "api",
		Usage: "minerva API mock server",
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

			&cli.IntFlag{
				Name:        "log-total",
				Aliases:     []string{"t"},
				Value:       100,
				Usage:       "Total number of Log",
				Destination: &handler.LogTotal,
			},
			&cli.IntFlag{
				Name:        "log-limit",
				Aliases:     []string{"l"},
				Value:       10,
				Usage:       "Limit of Log",
				Destination: &handler.LogLimit,
			},
		},

		Action: func(c *cli.Context) error {
			logger.WithFields(logrus.Fields{
				"handler": handler,
			}).Info("Start mock server")

			r := gin.Default()
			v1 := r.Group("/api/v1")
			api.SetupRoute(v1, handler)

			bindAddr := fmt.Sprintf("%s:%d", params.addr, params.port)
			return r.Run(bindAddr)
		},
	}

	if err := app.Run(os.Args); err != nil {
		logger.WithError(err).Fatal("Server error")
	}
}
