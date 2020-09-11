package main

import (
	"encoding/json"
	"fmt"

	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
	cli "github.com/urfave/cli/v2"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type dumpArguments struct {
	messageFiles cli.StringSlice
	indexFiles   cli.StringSlice
}

func dumpCommand(args *arguments) *cli.Command {
	var dumpArgs dumpArguments

	return &cli.Command{
		Name:  "dump",
		Usage: "Invoke merge process",
		Action: func(c *cli.Context) error {
			return dumpAction(*args, dumpArgs)
		},
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:        "index-file",
				Aliases:     []string{"i"},
				Usage:       "Index parquet file path",
				Destination: &dumpArgs.indexFiles,
			},
			&cli.StringSliceFlag{
				Name:        "message-file",
				Aliases:     []string{"m"},
				Usage:       "Message parquet file path",
				Destination: &dumpArgs.messageFiles,
			},
		},
	}
}

type newRecord func() models.Record
type readRecord func(pr *reader.ParquetReader) (models.Record, error)

func newIndexRecord() models.Record   { return &models.IndexRecord{} }
func newMessageRecord() models.Record { return &models.MessageRecord{} }
func readIndexRecord(pr *reader.ParquetReader) (models.Record, error) {
	records := make([]models.IndexRecord, 1)
	if err := pr.Read(&records); err != nil {
		return nil, err
	}
	return &records[0], nil
}
func readMessageRecord(pr *reader.ParquetReader) (models.Record, error) {
	records := make([]models.MessageRecord, 1)
	if err := pr.Read(&records); err != nil {
		return nil, err
	}
	return &records[0], nil
}

func dumpAction(args arguments, dumpArgs dumpArguments) error {
	for _, msgFile := range dumpArgs.messageFiles.Value() {
		if err := dumpParquetFile(msgFile, newMessageRecord, readMessageRecord); err != nil {
			return err
		}
	}

	for _, idxFile := range dumpArgs.indexFiles.Value() {
		if err := dumpParquetFile(idxFile, newIndexRecord, readIndexRecord); err != nil {
			return err
		}
	}

	return nil
}

func dumpParquetFile(filepath string, newRec newRecord, read readRecord) error {
	fr, err := local.NewLocalFileReader(filepath)
	if err != nil {
		return errors.Wrap(err, "Failed to open")
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, newRec(), 1)
	if err != nil {
		return err
	}
	defer pr.ReadStop()

	num := int(pr.GetNumRows())
	for i := 0; i < num; i++ {
		rec, err := read(pr)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		fmt.Println(string(raw))
	}

	return nil
}
