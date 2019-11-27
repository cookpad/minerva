package indexer

import (
	"fmt"

	"github.com/m-mizutani/minerva/internal"
	"github.com/pkg/errors"
)

type indexDumpers map[string]*indexDumper
type messageDumpers map[string]*messageDumper

type newDumperFunc func(dst internal.ParquetLocation) (dumper, error)
type dumpers map[string]dumper

func (x dumpers) dump(dst internal.ParquetLocation, r *logRecord, objID int64, newDumper newDumperFunc) error {
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

	if err := d.Dump(r, objID); err != nil {
		return errors.Wrapf(err, "Fail to dump record: %v", *r)
	}

	return nil
}

func newPqLoc(r *logRecord) (msgDst, idxDst internal.ParquetLocation) {
	dst := internal.ParquetLocation{
		MergeStat: internal.ParquetMergeStatUnmerged,
		Timestamp: r.Timestamp,
		SrcBucket: r.Src.Bucket,
		SrcKey:    r.Src.Key(),
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

		fmt.Println("recv records: ", len(q.Records))
		for _, r := range q.Records {
			objID, err := meta.GetObjecID(r.Src.Bucket, r.Src.Key())
			if err != nil {
				return nil, err
			}

			msgDst, idxDst := newPqLoc(&r)
			if err := dumperMap.dump(msgDst, &r, objID, newMessageDumper); err != nil {
				return nil, err
			}
			if err := dumperMap.dump(idxDst, &r, objID, newIndexDumper); err != nil {
				return nil, err
			}
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
