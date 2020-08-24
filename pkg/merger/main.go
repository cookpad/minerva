package merger

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/internal/service"
	"github.com/m-mizutani/minerva/pkg/handler"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/reader"
	"github.com/xitongsys/parquet-go/writer"
)

var logger = internal.Logger

func main() {
	handler.StartLambda(mergeHandler)
}

func mergeHandler(args handler.Arguments) error {
	records, err := args.DecapSQSEvent()
	if err != nil {
		return err
	}

	for _, record := range records {
		var q models.MergeQueue
		if err := record.Bind(&q); err != nil {
			return err
		}

		if err := MergeChunk(args, &q); err != nil {
			return errors.Wrap(err, "Failed composeChunka")
		}
	}

	return nil
}

// MergeChunk merges S3 objects to one parquet file
func MergeChunk(args handler.Arguments, q *models.MergeQueue) error {
	ch := make(chan *recordQueue)
	accessorMap := map[models.ParquetSchemaName]accessor{
		models.ParquetSchemaIndex:   &indexAccessor{},
		models.ParquetSchemaMessage: &messageAccessor{},
	}
	s3Service := args.S3Service()

	acr, ok := accessorMap[q.Schema]
	if !ok {
		logger.WithField("queue", q).Errorf("Unsupported schema: %s", q.Schema)
		return fmt.Errorf("Unsupported schema: %s", q.Schema)
	}

	var mergedFile *string
	var err error

	if len(q.SrcObjects) == 1 {
		mergedFile, err = downloadRecord(s3Service, *q.SrcObjects[0])
		if err != nil {
			return err
		}
	} else {
		go func() {
			defer close(ch)
			for _, src := range q.SrcObjects {
				loadRecord(s3Service, ch, *src, acr)
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

	if mergedFile != nil {
		defer os.Remove(*mergedFile)

		dst := models.NewS3Object(q.DstObject.Region, q.DstObject.Bucket, q.DstObject.Key)
		if err := s3Service.UploadFileToS3(*mergedFile, dst); err != nil {
			return err
		}
	}

	if err := s3Service.DeleteS3Objects(q.SrcObjects); err != nil {
		return err
	}

	return nil
}

type recordQueue struct {
	Err            error
	IndexRecords   []models.IndexRecord
	MessageRecords []models.MessageRecord
}

type accessor interface {
	newObj() interface{}
	read(*reader.ParquetReader, *recordQueue, int) error
	dump(*writer.ParquetWriter, *recordQueue) error
}

type indexAccessor struct{}

func (x *indexAccessor) newObj() interface{} { return new(models.IndexRecord) }
func (x *indexAccessor) read(pr *reader.ParquetReader, q *recordQueue, s int) error {
	rows := make([]models.IndexRecord, s)
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

func (x *messageAccessor) newObj() interface{} { return new(models.MessageRecord) }
func (x *messageAccessor) read(pr *reader.ParquetReader, q *recordQueue, s int) error {
	rows := make([]models.MessageRecord, s)
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

func downloadRecord(s3Svc *service.S3Service, src models.S3Object) (*string, error) {
	fname, err := s3Svc.DownloadS3Object(src)
	if err != nil {
		return nil, err
	}
	return fname, nil
}

func loadRecord(s3Svc *service.S3Service, ch chan *recordQueue, src models.S3Object, ac accessor) {
	fname, err := s3Svc.DownloadS3Object(src)
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
