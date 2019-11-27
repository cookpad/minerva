package indexer

import (
	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
)

type indexDumpers map[string]*indexDumper
type messageDumpers map[string]*messageDumper

type newDumperFunc func(dst internal.ParquetLocation) (dumper, error)
type dumpers map[string]dumper

func (x dumpers) dump(dst internal.ParquetLocation, q *logQueue, objID int64, newDumper newDumperFunc) error {
	pKey := dst.S3Key()
	d, ok := x[pKey]
	if !ok {
		newdump, err := newDumper(dst)
		if err != nil {
			return errors.Wrapf(err, "Fail to new dumper: %v", dst)
		}
		x[pKey] = newdump
		d = newdump
	}

	if err := d.Dump(q, objID); err != nil {
		return errors.Wrapf(err, "Fail to dump queue: %v", *q)
	}

	return nil
}

func newPqLoc(q *logQueue) (msgDst, idxDst internal.ParquetLocation) {
	dst := internal.ParquetLocation{
		MergeStat: internal.ParquetMergeStatUnmerged,
		Timestamp: q.Timestamp,
		SrcBucket: q.Src.Bucket,
		SrcKey:    q.Src.Key(),
	}

	// copy common variables
	msgDst = dst
	idxDst = dst

	msgDst.Schema = internal.ParquetSchemaMessage
	idxDst.Schema = internal.ParquetSchemaIndex
	return
}

// DumpParquetFiles dump log data to local parquet files
func DumpParquetFiles(ch chan *logQueue, meta internal.MetaAccessor) ([]dumper, error) {
	dumperMap := dumpers{}

	for q := range ch {
		if q.Err != nil {
			return nil, errors.Wrap(q.Err, "Fail to receive queue")
		}

		objID, err := meta.GetObjecID(q.Src.Bucket, q.Src.Key())
		if err != nil {
			return nil, err
		}

		msgDst, idxDst := newPqLoc(q)
		if err := dumperMap.dump(msgDst, q, objID, newMessageDumper); err != nil {
			return nil, err
		}
		if err := dumperMap.dump(idxDst, q, objID, newIndexDumper); err != nil {
			return nil, err
		}
	}

	var dumperList []dumper
	for _, d := range dumperMap {
		if err := d.Close(); err != nil {
			return nil, errors.Wrapf(err, "Fail to close dumper: %v", d)
		}
		dumperList = append(dumperList, d)
	}

	return dumperList, nil
}
