package indexer

import (
	"fmt"
	"io/ioutil"
	"os"

	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/source"
	"github.com/xitongsys/parquet-go/writer"
)

type dumper interface {
	Files() []*parquetFile
	Dump(q *logQueue, objID int64) error
	Close() error
	Delete() error
	Type() string
	Schema() internal.ParquetSchemaName
}

// baseDumper
type baseDumper struct {
	files    []*parquetFile
	current  *parquetFile
	baseDst  internal.ParquetLocation
	obj      interface{}
	dataSize int
}

type parquetFile struct {
	filePath string
	dst      internal.ParquetLocation
	dataSize int
	pw       *writer.ParquetWriter
	fw       source.ParquetFile
}

const (
	dumperTypeIndex   = "index"
	dumperTypeMessage = "message"
)

// DumperParquetSizeLimit specifies maximum parquet file size.
// When hitting the size, refresh (close & open another file).
var DumperParquetSizeLimit = 128 * 1000 * 1000 // 128MB

const (
	// About parquet format: https://parquet.apache.org/documentation/latest/
	parquetRowGroupSize = 16 * 1024 * 1024 // 16M
)

func (x *baseDumper) Files() []*parquetFile              { return x.files }
func (x *baseDumper) Schema() internal.ParquetSchemaName { return x.baseDst.Schema }

func (x *baseDumper) init(v interface{}, dst internal.ParquetLocation) error {
	x.obj = v
	x.baseDst = dst
	return x.open()
}

func (x *baseDumper) open() error {
	fd, err := ioutil.TempFile("", "*.parquet")
	if err != nil {
		return errors.Wrap(err, "Fail to create a temp parquet file")
	}
	fd.Close()
	filePath := fd.Name()

	x.current = &parquetFile{
		filePath: filePath,
		dst:      x.baseDst,
	}
	if len(x.files) > 0 {
		x.current.dst.FileNameSlat = fmt.Sprintf("%d", len(x.files))
	}
	x.files = append(x.files, x.current)

	logger.WithFields(logrus.Fields{
		"path": x.current.filePath,
		"dst":  x.current.dst,
	}).Debug("Open dumper")

	fw, err := local.NewLocalFileWriter(filePath)
	if err != nil {
		return errors.Wrap(err, "Fail to create a parquet file")
	}

	x.current.fw = fw

	pw, err := writer.NewParquetWriter(fw, x.obj, 4)
	if err != nil {
		return errors.Wrap(err, "Fail to create parquet writer")
	}

	pw.RowGroupSize = parquetRowGroupSize
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	x.current.pw = pw
	return nil
}

func (x *baseDumper) refresh(dataSize int) error {
	if x.dataSize+dataSize > DumperParquetSizeLimit {
		if err := x.Close(); err != nil {
			return err
		}

		if err := x.open(); err != nil {
			return err
		}
		x.dataSize = 0
	}
	x.dataSize += dataSize
	return nil
}

func (x *baseDumper) Close() error {
	logger.WithFields(logrus.Fields{
		"currentPath": x.current.filePath,
		"dst":         x.current.dst,
		"dataSize":    x.dataSize,
	}).Debug("Closing dumper")

	defer func() {
		x.current.fw.Close()
		x.current.fw = nil
		x.current.pw = nil
	}()

	if err := x.current.pw.WriteStop(); err != nil {
		// Logging at here because the error mey not be handled by defer call.
		logger.WithError(err).WithField("dumper", x).Error("Fail to WriteStop for IndexRecord")
		return errors.Wrap(err, "Fail to WriteStop for IndexRecord")
	}

	return nil
}

func (x *baseDumper) Delete() error {
	for _, f := range x.files {
		if err := os.Remove(f.filePath); err != nil {
			return err
		}
	}
	return nil
}

// ****************************************************
// indexDumper
type indexDumper struct {
	baseDumper
	tokenizer *internal.SimpleTokenizer
}

func newIndexDumper(dst internal.ParquetLocation) (dumper, error) {
	d := &indexDumper{
		tokenizer: internal.NewSimpleTokenizer(),
	}

	d.tokenizer.DisableRegex()

	if err := d.init(new(internal.IndexRecord), dst); err != nil {
		return nil, err
	}

	return d, nil
}

type indexTerm struct {
	field string
	term  string
}

func (x *indexDumper) Dump(q *logQueue, objID int64) error {
	terms := map[indexTerm]bool{}

	kvList := toKeyValuePairs(q.Value, "", false)

	for _, kv := range kvList {
		tokens := x.tokenizer.Split(fmt.Sprintf("%v", kv.Value))

		for _, token := range tokens {
			if token.IsDelim || token.IsSpace() {
				continue
			}

			t := indexTerm{field: kv.Key, term: token.Data}
			terms[t] = true
		}
	}

	for it := range terms {
		rec := internal.IndexRecord{
			Tag:       q.Tag,
			Timestamp: q.Timestamp.Unix(),
			Field:     it.field,
			Term:      it.term,
			ObjectID:  objID,
			Seq:       int32(q.Seq),
		}

		if err := x.refresh(len(q.Tag) + len(it.field) + len(it.term)); err != nil {
			return err
		}

		if err := x.current.pw.Write(rec); err != nil {
			return errors.Wrap(err, "Index write error")
		}
	}

	return nil
}

func (x *indexDumper) Type() string {
	return dumperTypeIndex
}

// ****************************************************
// messageDumper
type messageDumper struct {
	baseDumper
}

func newMessageDumper(dst internal.ParquetLocation) (dumper, error) {
	d := &messageDumper{}

	if err := d.init(new(internal.MessageRecord), dst); err != nil {
		return nil, err
	}

	return d, nil
}

func (x *messageDumper) Dump(q *logQueue, objID int64) error {
	rec := internal.MessageRecord{
		Timestamp: q.Timestamp.Unix(),
		Message:   q.Message,
		ObjectID:  objID,
		Seq:       q.Seq,
	}

	if err := x.refresh(len(q.Tag) + len(q.Message)); err != nil {
		return err
	}

	if err := x.current.pw.Write(rec); err != nil {
		return errors.Wrap(err, "Index write error")
	}

	return nil
}

func (x *messageDumper) Type() string {
	return dumperTypeMessage
}
