package main

import (
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
)

type arguments struct {
	Region         string
	MetaTableName  string
	AthenaDBName   string
	OutputLocation string
	Queue          internal.PartitionQueue
}

func makePartition(args arguments) error {
	meta := internal.NewMetaDynamoDB(args.Region, args.MetaTableName)

	if err := createPartition(args.Region, args.AthenaDBName, args.Queue, meta, args.OutputLocation); err != nil {
		return errors.Wrapf(err, "Fail to create partition: %v", args)
	}

	return nil
}
