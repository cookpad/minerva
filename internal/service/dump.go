package service

import (
	"io"

	"github.com/m-mizutani/minerva/internal/adaptor"
	"github.com/m-mizutani/minerva/internal/transform"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
)

type DumpService struct {
	objectID   int64
	dstBase    models.S3Object
	newS3      adaptor.S3ClientFactory
	newEncoder adaptor.EncoderFactory

	dumpers dumperMap
}

func NewDumpService(objectID int64, dstBase models.S3Object, newS3 adaptor.S3ClientFactory, newEncoder adaptor.EncoderFactory) *DumpService {
	return &DumpService{
		objectID:   objectID,
		dstBase:    dstBase,
		newS3:      newS3,
		newEncoder: newEncoder,
	}
}

func (x *DumpService) Dump(q *models.LogQueue) error {
	s3svc := NewS3Service(x.newS3)
	prefix := models.NewRawObjectPrefix(models.ParquetSchemaIndex, x.dstBase, q.Src, q.Timestamp)

	dumper := x.dumpers.getDumper(prefix, x.newEncoder, s3svc, x.objectID)

	if err := dumper.dump(q); err != nil {
		return err
	}

	return nil
}

func (x *DumpService) Close(q *models.LogQueue) error {
	for _, dumper := range x.dumpers {
		if err := dumper.close(); err != nil {
			return err
		}
	}

	return nil
}

func (x *DumpService) RawObjects() []*models.RawObject {
	return nil
}

type dumperMap map[string]*dumper

func (x dumperMap) getDumper(prefix *models.RawObjectPrefix, newEncoder adaptor.EncoderFactory, s3Service *S3Service, objID int64) *dumper {
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

	return &dumper{
		prefix:       prefix,
		newEncoder:   newEncoder,
		s3Service:    s3Service,
		transformLog: logTransform,
		objectID:     objID,
	}
}

type pipeline struct {
	encoder    adaptor.Encoder
	rawObject  *models.RawObject
	pipeWriter io.WriteCloser
}

type dumper struct {
	prefix       *models.RawObjectPrefix
	newEncoder   adaptor.EncoderFactory
	s3Service    *S3Service
	transformLog transform.LogToRecord
	objectID     int64

	current   *pipeline
	pipelines []*pipeline
}

func (x *dumper) newPipeline() *pipeline {
	pr, pw := io.Pipe()
	encoder := x.newEncoder(pw)
	rawObject := models.NewRawObject(x.prefix, encoder.Ext())

	go x.s3Service.Upload(pr, *rawObject.Object(), encoder.ContentEncoding())

	pline := &pipeline{
		encoder:    encoder,
		rawObject:  rawObject,
		pipeWriter: pw,
	}
	x.current = pline
	x.pipelines = append(x.pipelines, pline)

	return pline
}

func (x *dumper) dump(q *models.LogQueue) error {
	recrods, err := x.transformLog(q, x.objectID)
	if err != nil {
		return errors.Wrap(err, "Failed transformLog")
	}

	pline := x.current
	if pline == nil {
		pline = x.newPipeline()
	}

	for _, record := range recrods {
		if err := pline.encoder.Encode(record); err != nil {
			return errors.Wrap(err, "Failed pipeline.encoder.Encode")
		}
	}

	return nil
}

func (x *dumper) close() error {
	for _, pline := range x.pipelines {
		if err := pline.encoder.Close(); err != nil {
			return errors.Wrap(err, "Failed pipeline.encoder.Close")
		}

		if err := pline.pipeWriter.Close(); err != nil {
			return errors.Wrap(err, "Failed pline.pipeWriter.Close")
		}

		pline.rawObject.DataSize = pline.encoder.Size()
	}

	return nil
}
