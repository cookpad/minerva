package indexer

import (
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"

	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type dumper interface {
	Files() []*csvFile
	Dump(q *logQueue, objID int64) error
	Close() error
	Delete() error
	Type() string
	Schema() internal.ParquetSchemaName
}

// baseDumper
type baseDumper struct {
	files    []*csvFile
	current  *csvFile
	baseDst  internal.ParquetLocation
	obj      interface{}
	dataSize int
}

type csvFile struct {
	filePath string
	dst      internal.ParquetLocation
	dataSize int
	fw       *os.File
	gw       *gzip.Writer
	cw       *csv.Writer
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
	parquetRowGroupSize = 16 * 1024 * 1024 //16M
)

func (x *baseDumper) Files() []*csvFile                  { return x.files }
func (x *baseDumper) Schema() internal.ParquetSchemaName { return x.baseDst.Schema }

func (x *baseDumper) init(v interface{}, dst internal.ParquetLocation) error {
	x.obj = v
	x.baseDst = dst
	return x.open()
}

func (x *baseDumper) open() error {
	fd, err := ioutil.TempFile("", "*.csv.gz")
	if err != nil {
		return errors.Wrap(err, "Fail to create a temp parquet file")
	}
	filePath := fd.Name()

	x.current = &csvFile{
		filePath: filePath,
		fw:       fd,
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

	gw := gzip.NewWriter(fd)

	x.current.gw = gw

	x.current.cw = csv.NewWriter(gw)
	return nil
}

func (x *baseDumper) refresh(dataSize int) error {
	if x.dataSize+dataSize > DumperParquetSizeLimit {
		if err := x.Close(); err != nil {
			return err
		}

		runtime.GC()

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

	x.current.cw.Flush()
	if err := x.current.gw.Close(); err != nil {
		return errors.Wrapf(err, "Fail to close gzip: %v", *x)
	}
	if err := x.current.fw.Close(); err != nil {
		return errors.Wrapf(err, "Fail to close file: %v", *x)
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
		if err := x.refresh(len(q.Tag) + len(it.field) + len(it.term)); err != nil {
			return err
		}

		row := []string{
			q.Tag,
			fmt.Sprintf("%d", objID),
			fmt.Sprintf("%d", q.Seq),
			it.field,
			it.term,
		}

		if err := x.current.cw.Write(row); err != nil {
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
	if err := x.refresh(len(q.Tag) + len(q.Message)); err != nil {
		return err
	}

	row := []string{
		fmt.Sprintf("%d", q.Timestamp.Unix()),
		fmt.Sprintf("%d", objID),
		fmt.Sprintf("%d", q.Seq),
		q.Message,
	}

	if err := x.current.cw.Write(row); err != nil {
		return errors.Wrap(err, "Message write error")
	}

	return nil
}

func (x *messageDumper) Type() string {
	return dumperTypeMessage
}
