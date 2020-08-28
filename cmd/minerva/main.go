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
		Commands: []*cli.Command{
			mergeCommand(&args),
			proxyCommand(&args),
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		logger.WithError(err).Fatal("Abort")
	}
}
