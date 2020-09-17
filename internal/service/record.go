package service

import (
	"io"
	"sync"

	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/transform"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type RecordService struct {
	ObjectSizeLimit int64

	s3Service  *S3Service
	newEncoder adaptor.EncoderFactory
	newDecoder adaptor.DecoderFactory

	dumpers dumperMap
}

func NewRecordService(newS3 adaptor.S3ClientFactory, newEncoder adaptor.EncoderFactory, newDecoder adaptor.DecoderFactory) *RecordService {
	return &RecordService{
		ObjectSizeLimit: 200 * 1024 * 1024,
		s3Service:       NewS3Service(newS3),
		newEncoder:      newEncoder,
		newDecoder:      newDecoder,
		dumpers:         dumperMap{},
	}
}

func (x *RecordService) Dump(q *models.LogQueue, objectID int64, dstBase *models.S3Object) error {
	prefixs := []*models.RawObjectPrefix{
		models.NewRawObjectPrefix(models.ParquetSchemaIndex, *dstBase, q.Src, q.Timestamp),
		models.NewRawObjectPrefix(models.ParquetSchemaMessage, *dstBase, q.Src, q.Timestamp),
	}

	for _, prefix := range prefixs {
		dumper := x.dumpers.getDumper(prefix, x.newEncoder, x.s3Service, objectID, x.ObjectSizeLimit)
		if err := dumper.dump(q); err != nil {
			return err
		}
	}

	return nil
}

func (x *RecordService) Close() error {
	for _, dumper := range x.dumpers {
		if err := dumper.close(); err != nil {
			return err
		}
	}

	return nil
}

func (x *RecordService) Load(src *models.S3Object, schema models.ParquetSchemaName, ch chan *models.RecordQueue) error {
	body, err := x.s3Service.AsyncDownload(*src)
	if err != nil {
		return errors.Wrap(err, "Failed AsyncDownload")
	}
	defer body.Close()

	decoder := x.newDecoder(body)
	for {
		var record models.Record
		switch schema {
		case models.ParquetSchemaIndex:
			record = &models.IndexRecord{}
		case models.ParquetSchemaMessage:
			record = &models.MessageRecord{}
		default:
			logger.Fatalf("Unsupported schema '%v' in RecordService.Load", schema)
		}

		if err := decoder.Decode(&record); err != nil {
			if err != io.EOF {
				return errors.Wrap(err, "Failed to decode record")
			}
			return nil
		}

		q := &models.RecordQueue{}
		q.Records = append(q.Records, record)

		ch <- q
	}
}

func (x *RecordService) RawObjects() []*models.RawObject {
	var objects []*models.RawObject
	for _, d := range x.dumpers {
		for _, p := range d.pipelines {
			objects = append(objects, p.rawObject)
		}
	}
	return objects
}

// ------------------------------------------------------------
// dumper has multiple pipeline to split too large log object
//
type dumper struct {
	prefix       *models.RawObjectPrefix
	newEncoder   adaptor.EncoderFactory
	s3Service    *S3Service
	transformLog transform.LogToRecord
	objectID     int64

	current   *pipeline
	pipelines []*pipeline

	sizeLimit int64
}

func (x *dumper) renewPipeline() (*pipeline, error) {
	pline := newPipeline(x.prefix, x.s3Service, x.newEncoder)
	if x.current != nil {
		logger.WithFields(logrus.Fields{
			"size":  x.current.encoder.Size(),
			"limit": x.sizeLimit,
		}).Debug("renew pipeline")

		if err := x.current.close(); err != nil {
			return nil, err
		}
	}

	x.current = pline
	x.pipelines = append(x.pipelines, pline)

	return pline, nil
}

func (x *dumper) dump(q *models.LogQueue) error {
	recrods, err := x.transformLog(q, x.objectID)
	if err != nil {
		return errors.Wrap(err, "Failed transformLog")
	}

	pline := x.current
	if pline == nil || pline.encoder.Size() > x.sizeLimit {
		pline, err = x.renewPipeline()
		if err != nil {
			return errors.Wrap(err, "Failed newPipeline")
		}
	}

	for _, record := range recrods {
		if err := pline.encoder.Encode(record); err != nil {
			return errors.Wrap(err, "Failed pipeline.encoder.Encode")
		}
	}

	return nil
}

func (x *dumper) close() error {
	if err := x.current.close(); err != nil {
		return err
	}

	x.current = nil

	return nil
}

type dumperMap map[string]*dumper

func (x dumperMap) getDumper(prefix *models.RawObjectPrefix, newEncoder adaptor.EncoderFactory, s3Service *S3Service, objID, sizeLimit int64) *dumper {
	d, ok := x[prefix.Key()]
	if ok {
		return d
	}

	var logTransform transform.LogToRecord
	switch prefix.Schema() {
	case models.ParquetSchemaIndex:
		logTransform = transform.LogToIndexRecord
	case models.ParquetSchemaMessage:
		logTransform = transform.LogToMessageRecord
	default:
		logger.Fatalf("unsupported schema name in getDumper: %s", prefix.Schema())
	}

	d = &dumper{
		prefix:       prefix,
		newEncoder:   newEncoder,
		s3Service:    s3Service,
		transformLog: logTransform,
		objectID:     objID,
		sizeLimit:    sizeLimit,
	}
	x[prefix.Key()] = d

	return d
}

// ------------------------------------------------------------
// dumper has multiple pipeline to split too large log object
//

type pipeline struct {
	encoder    adaptor.Encoder
	rawObject  *models.RawObject
	pipeWriter io.WriteCloser
	wg         *sync.WaitGroup
}

func newPipeline(prefix *models.RawObjectPrefix, s3Service *S3Service, newEncoder adaptor.EncoderFactory) *pipeline {
	pr, pw := io.Pipe()
	encoder := newEncoder(pw)
	rawObject := models.NewRawObject(prefix, encoder.Ext())

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		obj := rawObject.Object()
		if err := s3Service.AsyncUpload(pr, *rawObject.Object(), encoder.ContentEncoding()); err != nil {
			logger.WithError(err).Error("Failed AsyncUpload")
		}
		logger.WithField("obj", obj).Info("Done upload")
	}()

	pline := &pipeline{
		encoder:    encoder,
		rawObject:  rawObject,
		pipeWriter: pw,
		wg:         wg,
	}

	return pline
}

func (x *pipeline) close() error {
	if err := x.encoder.Close(); err != nil {
		return errors.Wrap(err, "Failed pipeline.encoder.Close")
	}

	if err := x.pipeWriter.Close(); err != nil {
		return errors.Wrap(err, "Failed pline.pipeWriter.Close")
	}

	x.wg.Wait()
	x.rawObject.DataSize = x.encoder.Size()
	logger.WithField("rawObject", x.rawObject).Debug("Closed pipeline")

	return nil
}
