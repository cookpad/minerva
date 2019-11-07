package main

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

type arguments struct {
	Queue     internal.MergeQueue
	NotDelete bool
}

type recordQueue struct {
	Err            error
	IndexRecords   []internal.IndexRecord
	MessageRecords []internal.MessageRecord
}

type accessor interface {
	newObj() interface{}
	read(*reader.ParquetReader, *recordQueue, int) error
	dump(*writer.ParquetWriter, *recordQueue) error
}

type indexAccessor struct{}

func (x *indexAccessor) newObj() interface{} { return new(internal.IndexRecord) }
func (x *indexAccessor) read(pr *reader.ParquetReader, q *recordQueue, s int) error {
	rows := make([]internal.IndexRecord, s)
	if err := pr.Read(&rows); err != nil {
		return err
	}
	q.IndexRecords = rows
	return nil
}
func (x *indexAccessor) dump(pw *writer.ParquetWriter, q *recordQueue) error {
	for i := range q.IndexRecords {
		if err := pw.Write(q.IndexRecords[i]); err != nil {
			return errors.Wrapf(err, "Fail to write Index record: %v", q.IndexRecords[i])
		}
	}

	return nil
}

type messageAccessor struct{}

func (x *messageAccessor) newObj() interface{} { return new(internal.MessageRecord) }
func (x *messageAccessor) read(pr *reader.ParquetReader, q *recordQueue, s int) error {
	rows := make([]internal.MessageRecord, s)
	if err := pr.Read(&rows); err != nil {
		return err
	}
	q.MessageRecords = rows
	return nil
}
func (x *messageAccessor) dump(pw *writer.ParquetWriter, q *recordQueue) error {
	for i := range q.MessageRecords {
		if err := pw.Write(q.MessageRecords[i]); err != nil {
			return errors.Wrapf(err, "Fail to write message record: %v", q.MessageRecords[i])
		}
	}

	return nil
}

func downloadRecord(src internal.S3Location) (*string, error) {
	fname, err := internal.DownloadS3Object(src.Region, src.Bucket, src.Key)
	if err != nil {
		return nil, err
	}
	return fname, nil
}

func loadRecord(ch chan *recordQueue, src internal.S3Location, ac accessor) {
	fname, err := internal.DownloadS3Object(src.Region, src.Bucket, src.Key)
	if err != nil {
		ch <- &recordQueue{Err: err}
		return
	}
	if fname == nil {
		return // Object not found
	}
	defer os.Remove(*fname)

	fr, err := local.NewLocalFileReader(*fname)
	if err != nil {
		ch <- &recordQueue{Err: errors.Wrapf(err, "Fail to new local file reader: %v", src)}
		return
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, ac.newObj(), 1)
	if err != nil {
		ch <- &recordQueue{Err: errors.Wrapf(err, "Fail to new parquet reader: %v", src)}
		return
	}
	defer pr.ReadStop()

	batchSize := 256
	num := int(pr.GetNumRows())
	for i := 0; i < num; i += batchSize {
		q := &recordQueue{}
		if err := ac.read(pr, q, batchSize); err != nil {
			ch <- &recordQueue{Err: errors.Wrap(err, "Fail to read index records from parquet file")}
			return
		}
		ch <- q
	}
	return
}

func dumpParquet(ch chan *recordQueue, ac accessor) (*string, error) {
	fd, err := ioutil.TempFile("", "*.parquet")
	if err != nil {
		return nil, errors.Wrap(err, "Fail to create a temp parquet file")
	}
	fd.Close()
	filePath := fd.Name()

	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return nil, errors.Wrap(err, "Fail to create a parquet file")
	}
	defer func() {
		logger.Debug("Closing parquet writer...")
		if err := fw.Close(); err != nil {
			logger.WithError(err).Error("Fail to close parquet writer")
		}
	}()

	pw, err := writer.NewParquetWriter(fw, ac.newObj(), 1)
	if err != nil {
		return nil, errors.Wrap(err, "Fail to create parquet writer")
	}
	defer func() {
		logger.Debug("Stopping parquet writer...")
		if err := pw.WriteStop(); err != nil {
			logger.WithError(err).Error("Fail to stop writing parquet file")
		}
	}()

	pw.RowGroupSize = 16 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	queueCount := 0
	logger.Debug("Start dumping")
	for q := range ch {
		if q.Err != nil {
			return nil, errors.Wrap(q.Err, "Fail to load IndexRecord")
		}

		queueCount++

		if err := ac.dump(pw, q); err != nil {
			return nil, err
		}
	}
	logger.WithField("queueCount", queueCount).Debugf("Dumped indices: %v", filePath)

	return &filePath, nil
}

func mergeParquet(args arguments) error {
	ch := make(chan *recordQueue)
	accessorMap := map[internal.ParquetSchemaName]accessor{
		internal.ParquetSchemaIndex:   &indexAccessor{},
		internal.ParquetSchemaMessage: &messageAccessor{},
	}

	acr, ok := accessorMap[args.Queue.Schema]
	if !ok {
		logger.WithField("queue", args.Queue).Errorf("Unsupported schema: %s", args.Queue.Schema)
		return fmt.Errorf("Unsupported schema: %s", args.Queue.Schema)
	}

	var mergedFile *string
	var err error

	if len(args.Queue.SrcObjects) == 1 {
		mergedFile, err = downloadRecord(args.Queue.SrcObjects[0])
		if err != nil {
			return err
		}
	} else {
		go func() {
			defer close(ch)
			for _, src := range args.Queue.SrcObjects {
				loadRecord(ch, src, acr)
			}
		}()

		mergedFile, err = dumpParquet(ch, acr)
		if err != nil {
			return err
		}
		if mergedFile == nil {
			logger.Warn("No available merged file")
			return nil
		}
	}
	defer os.Remove(*mergedFile)

	if err := internal.UploadFileToS3(*mergedFile, args.Queue.DstObject.Region, args.Queue.DstObject.Bucket, args.Queue.DstObject.Key); err != nil {
		return err
	}

	if err := internal.DeleteS3Objects(args.Queue.SrcObjects); err != nil {
		return err
	}

	return nil
}
