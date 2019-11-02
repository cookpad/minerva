package indexer

import (
	"fmt"
	"time"
)

type s3Partition struct {
	Dst       s3Loc
	Src       s3Loc
	Timestamp time.Time
	Tag       string
}

func (x s3Partition) partitionPrefix() string {
	return fmt.Sprintf("%sindices/tg=%s/dt=%s/", x.Dst.Prefix, x.tagKey(), x.dateKey())
}

func (x s3Partition) fullKey() string {
	return fmt.Sprintf("%sunmerged/%02d/%s/%s.parquet", x.partitionPrefix(),
		x.Timestamp.Hour(), x.Src.Bucket, x.Src.Key())
}

func (x s3Partition) dateKey() string {
	return x.Timestamp.Format("2006-01-02")
}

func (x s3Partition) tagKey() string {
	return x.Tag
}

func (x s3Partition) outputPrefix() string {
	return fmt.Sprintf("%soutput/", x.Dst.Prefix)
}
