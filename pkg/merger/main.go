package merger

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

var logger = handler.Logger

// MergeOptions has option values of MergeChunk
type MergeOptions struct {
	DoNotRemoveSrc     bool
	DoNotRemoveParquet bool
}

// MergeChunk merges S3 objects to one parquet file
func MergeChunk(args handler.Arguments, q *models.MergeQueue, opt *MergeOptions) error {
	if opt == nil {
		opt = &MergeOptions{}
	}

	recordService := args.RecordService()
	ch := make(chan *models.RecordQueue)
	newRecordMap := map[models.ParquetSchemaName]newRecord{
		models.ParquetSchemaIndex:   newIndexRecord,
		models.ParquetSchemaMessage: newMessageRecord,
	}
	s3Service := args.S3Service()

	newRec, ok := newRecordMap[q.Schema]
	if !ok {
		logger.WithField("queue", q).Errorf("Unsupported schema: %s", q.Schema)
		return fmt.Errorf("Unsupported schema: %s", q.Schema)
	}

	var mergedFile *string
	var err error

	go func() {
		defer close(ch)
		for _, src := range q.SrcObjects {
			logger.WithField("src", src).Debug("Download raw object")
			if err := recordService.Load(src, q.Schema, ch); err != nil {
				logger.WithError(err).WithField("src", src).Fatal("Failed to load records")
				return
			}
		}
	}()

	mergedFile, err = dumpParquet(ch, newRec)
	if err != nil {
		return err
	}
	if mergedFile == nil {
		logger.Warn("No available merged file")
		return nil
	}

	logger.WithField("mergedFile", *mergedFile).Debug("Merged records")
	if !opt.DoNotRemoveParquet {
		defer os.Remove(*mergedFile)
	}

	dst := models.NewS3Object(q.DstObject.Region, q.DstObject.Bucket, q.DstObject.Key)
	if err := s3Service.UploadFileToS3(*mergedFile, dst); err != nil {
		return err
	}

	logger.WithField("dst", q.DstObject).Debug("Uploaded merged parquet file")
	if !opt.DoNotRemoveSrc {
		if err := s3Service.DeleteS3Objects(q.SrcObjects); err != nil {
			return err
		}
	}

	return nil
}

func dumpParquet(ch chan *models.RecordQueue, newRec newRecord) (*string, error) {
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

	pw, err := writer.NewParquetWriter(fw, newRec(), 1)
	if err != nil {
		return nil, errors.Wrap(err, "Fail to create parquet writer")
	}
	defer func() {
		logger.Debug("Stopping parquet writer...")

		if err := pw.WriteStop(); err != nil {
			logger.WithError(err).Error("Fail to stop writing parquet file")
		}
	}()

	pw.RowGroupSize = 128 * 1024 * 1024
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	queueCount := 0
	logger.Debug("Start dumping")
	for q := range ch {
		if q.Err != nil {
			return nil, errors.Wrap(q.Err, "Fail to load IndexRecord")
		}

		queueCount++

		for i := range q.Records {
			if err := pw.Write(q.Records[i]); err != nil {
				return nil, errors.Wrapf(err, "Fail to write record as parquet: %v", q.Records[i])
			}
		}

		if queueCount%10000 == 0 {
			runtime.GC()
		}
	}
	logger.WithField("queueCount", queueCount).Debugf("Dumped records: %v", filePath)

	return &filePath, nil
}

type newRecord func() interface{}

func newIndexRecord() interface{}   { return new(models.IndexRecord) }
func newMessageRecord() interface{} { return new(models.MessageRecord) }

/*
type accessor interface {
	dump(*writer.ParquetWriter, *models.RecordQueue) error
}

type indexAccessor struct{}

func (x *indexAccessor) dump(pw *writer.ParquetWriter, q *models.RecordQueue) error {
	for i := range q.IndexRecords {
		if err := pw.Write(q.IndexRecords[i]); err != nil {
			return errors.Wrapf(err, "Fail to write Index record: %v", q.IndexRecords[i])
		}
	}

	return nil
}

type messageAccessor struct{}

func (x *messageAccessor) newObj() interface{} { return new(models.MessageRecord) }
func (x *messageAccessor) read(pr *reader.ParquetReader, q *models.RecordQueue, s int) error {
	rows := make([]models.MessageRecord, s)
	if err := pr.Read(&rows); err != nil {
		return err
	}
	q.MessageRecords = rows
	return nil
}
func (x *messageAccessor) dump(pw *writer.ParquetWriter, q *models.RecordQueue) error {
	for i := range q.MessageRecords {
		if err := pw.Write(q.MessageRecords[i]); err != nil {
			return errors.Wrapf(err, "Fail to write message record: %v", q.MessageRecords[i])
		}
	}

	return nil
}

func downloadRecord(s3Svc *service.S3Service, src models.S3Object) (*string, error) {
	fname, err := s3Svc.DownloadS3Object(src)
	if err != nil {
		return nil, err
	}
	return fname, nil
}

func loadRecord(s3Svc *service.S3Service, ch chan *models.RecordQueue, src models.S3Object, ac accessor) {
	fname, err := s3Svc.DownloadS3Object(src)
	if err != nil {
		ch <- &models.RecordQueue{Err: err}
		return
	}
	if fname == nil {
		return // Object not found
	}
	defer os.Remove(*fname)

	fr, err := local.NewLocalFileReader(*fname)
	if err != nil {
		ch <- &models.RecordQueue{Err: errors.Wrapf(err, "Fail to new local file reader: %v", src)}
		return
	}
	defer fr.Close()

	pr, err := reader.NewParquetReader(fr, ac.newObj(), 1)
	if err != nil {
		ch <- &models.RecordQueue{Err: errors.Wrapf(err, "Fail to new parquet reader: %v", src)}
		return
	}
	defer pr.ReadStop()

	batchSize := 256
	num := int(pr.GetNumRows())
	for i := 0; i < num; i += batchSize {
		q := &models.RecordQueue{}
		if err := ac.read(pr, q, batchSize); err != nil {
			ch <- &models.RecordQueue{Err: errors.Wrap(err, "Fail to read index records from parquet file")}
			return
		}
		ch <- q
	}
	return
}
*/
