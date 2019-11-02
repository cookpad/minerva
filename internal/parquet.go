package internal

import (
	"crypto/sha1"
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
)

// ParquetSchemaName identifies schema name
type ParquetSchemaName string

const (
	// ParquetSchemaIndex indicates IndexRecord parquet schema
	ParquetSchemaIndex ParquetSchemaName = "index"
	// ParquetSchemaMessage indicates MessageRecord
	ParquetSchemaMessage ParquetSchemaName = "message"
)

// ParquetMergeStat indicates path of merged/unmerged objects
type ParquetMergeStat string

const (
	// ParquetMergeStatMerged indicates merged object path
	ParquetMergeStatMerged ParquetMergeStat = "merged"
	// ParquetMergeStatUnmerged indicates unmerged object path
	ParquetMergeStatUnmerged ParquetMergeStat = "unmerged"
)

// IndexRecord is used for inverted index of log files on S3 bucket.
type IndexRecord struct {
	// Tag should exist in indexRecord even if parition contains tag (tg) as key.
	// Because a number of Athena table partition is up to 20,000 and
	// minerva can have only about 50 tags for 1 year (365 days * 50 tags = 18250)
	// Then I need to consider a case that partition "tg" can not be used and
	// indexRecord should have Tag field.
	Tag      string `parquet:"name=tag, type=UTF8, encoding=PLAIN_DICTIONARY" json:"tag"`
	Field    string `parquet:"name=field, type=UTF8, encoding=PLAIN_DICTIONARY" json:"field"`
	Term     string `parquet:"name=term, type=UTF8, encoding=PLAIN_DICTIONARY" json:"term"`
	ObjectID int64  `parquet:"name=object_id, type=INT64" json:"object_id"`
	Seq      int32  `parquet:"name=seq, type=INT32" json:"seq"`
}

// ObjectRecord has mapping from ObjectID to S3Bucket and S3Key to reduce index parquet size.
type ObjectRecord struct {
	ObjectID int64  `parquet:"name=object_id, type=INT64" json:"object_id"`
	S3Bucket string `parquet:"name=s3_bucket, type=UTF8, encoding=PLAIN_DICTIONARY" json:"s3_bucket"`
	S3Key    string `parquet:"name=s3_key, type=UTF8, encoding=PLAIN_DICTIONARY" json:"s3_key"`
}

// MessageRecord stores original log message that is encoded to JSON.
type MessageRecord struct {
	// Timestamp is unixtime (second) of original log.
	Timestamp int64  `parquet:"name=timestamp, type=INT64" json:"timestamp"`
	ObjectID  int64  `parquet:"name=object_id, type=INT64" json:"object_id"`
	Seq       int32  `parquet:"name=seq, type=INT32" json:"seq"`
	Message   string `parquet:"name=message, type=UTF8, encoding=PLAIN_DICTIONARY" json:"message"`
}

const (
	s3DirNameIndex    = "indices"
	s3DirNameMessage  = "messages"
	s3DirNameMerged   = "merged"
	s3DirNameUnmerged = "unmerged"
)

// ParquetLocation indicates S3 path of parquet file. Minerva defines a path rule
// for parquet files on S3 and ParquetLocation includes logics of the rule.
//
// Key Format:
// s3://{bucket}/{prefix}{schema}/{partition}/{merged,unmerged}/{hour}/{srcBucket}/{srcKey}.parquet
type ParquetLocation struct {
	Region       string
	Bucket       string
	Prefix       string
	MergeStat    ParquetMergeStat
	Schema       ParquetSchemaName
	Timestamp    time.Time
	Tag          string
	SrcBucket    string
	SrcKey       string
	FileNameSlat string
}

// S3Key returns full S3 key of the parquet object on S3.
func (x ParquetLocation) S3Key() string {
	key := x.Prefix + strings.Join([]string{
		x.schemaName(),
		x.Partition(),
		string(x.MergeStat),
		x.Timestamp.Format("15"),
	}, "/")

	if x.SrcBucket != "" {
		key += "/" + x.SrcBucket
	}

	key += "/" + x.SrcKey
	if !strings.HasSuffix(key, ".parquet") {
		// Avoid file name conflict.
		if x.FileNameSlat != "" {
			v := x.SrcKey + x.FileNameSlat
			h := sha1.New()
			h.Write([]byte(v))
			key += fmt.Sprintf(".%x", h.Sum(nil))
		}
		key += ".parquet"
	}

	return key
}

func (x ParquetLocation) schemaName() string {
	switch x.Schema {
	case ParquetSchemaIndex:
		return s3DirNameIndex
	case ParquetSchemaMessage:
		return s3DirNameMessage
	default:
		Logger.WithField("location", x).Fatalf("Invalid schema: %v", x.Schema)
		return ""
	}
}

// TableName retruns Athena table name according to schema
func (x ParquetLocation) TableName() string {
	return x.schemaName()
}

// PartitionPrefix returns a part of partition path. Main purpose is to manage
// dumper
func (x ParquetLocation) PartitionPrefix() string {
	return x.Prefix + strings.Join([]string{
		x.schemaName(),
		x.Partition(),
	}, "/")
}

// PartitionAndMergeStat returns a part of partition path. Main purpose is to manage
// dumper
func (x ParquetLocation) PartitionAndMergeStat() string {
	return x.Prefix + strings.Join([]string{
		x.schemaName(),
		x.Partition(),
		string(x.MergeStat),
	}, "/")
}

// PartitionSchemaPrefix returns a part of partition path including only schema.
// The function is for ListObjects to list tags
func (x ParquetLocation) PartitionSchemaPrefix() string {
	return x.Prefix + strings.Join([]string{
		x.schemaName(),
	}, "/")
}

// PartitionLocation returns partition key part of S3 location.
// The function is for creating partition by ALTER TABLE.
func (x ParquetLocation) PartitionLocation() string {
	return "s3://" + x.Bucket + "/" + x.PartitionPrefix() + "/"
}

// PartitionKeys returns map of key set for Athena partitioning.
func (x ParquetLocation) PartitionKeys() map[string]string {
	return map[string]string{
		"dt": x.DtKey(),
		"tg": x.TgKey(),
	}
}

// DtKey returns date key for "dt="
func (x ParquetLocation) DtKey() string {
	return x.Timestamp.Format("2006-01-02")
}

// TgKey returns tag key for "tg="
func (x ParquetLocation) TgKey() string {
	return x.Tag
}

func (x ParquetLocation) partitionDate() string {
	return "dt=" + x.DtKey()
}

func (x ParquetLocation) partitionTag() string {
	return "tg=" + x.TgKey()
}

// Partition returns a partition related part of S3 key.
func (x ParquetLocation) Partition() string {
	return x.partitionTag() + "/" + x.partitionDate()
}

// ParseS3Key parses S3 key to generate a new ParquetLocation
func ParseS3Key(key, prefix string) (*ParquetLocation, error) {
	loc := ParquetLocation{
		Prefix: prefix,
	}
	// s3://{bucket}/{prefix}{schema}/{partition}/{merged,unmerged}/{hour}/{srcBucket}/{srcKey}.parquet

	if !strings.HasPrefix(key, prefix) {
		return nil, fmt.Errorf("Prefix is not matched: %s %s", prefix, key)
	}

	suffixKey := key[len(prefix):]
	arr := strings.Split(suffixKey, "/")

	if len(arr) > 0 {
		switch arr[0] {
		case s3DirNameIndex:
			loc.Schema = ParquetSchemaIndex
		case s3DirNameMessage:
			loc.Schema = ParquetSchemaMessage
		default:
			return nil, fmt.Errorf("Invalid schema name, must be %s or %s: %v", s3DirNameIndex, ParquetSchemaMessage, arr[0])
		}
	}

	// tg key
	var timestamp string
	if len(arr) > 1 && arr[1] != "" {
		v := arr[1]
		if !strings.HasPrefix(v, "tg=") {
			return nil, fmt.Errorf("Invalid partition key (tg): %v", v)
		}

		loc.Tag = v[len("tg="):]

	}

	// dt key
	if len(arr) > 2 && arr[2] != "" {
		v := arr[2]
		if !strings.HasPrefix(v, "dt=") {
			return nil, fmt.Errorf("Invalid partition key (dt): %v", v)
		}

		timestamp = v[len("dt="):]
	}

	// merge status
	if len(arr) > 3 && arr[3] != "" {
		v := arr[3]
		switch v {
		case s3DirNameMerged:
			loc.MergeStat = ParquetMergeStatMerged
		case s3DirNameUnmerged:
			loc.MergeStat = ParquetMergeStatUnmerged
		default:
			return nil, fmt.Errorf("Invalid merge status: %v", v)
		}
	}

	// hour
	if timestamp != "" {
		if len(arr) > 4 && arr[4] != "" {
			timestamp += " " + arr[4]
		} else {
			timestamp += " 00"
		}

		ts, err := time.Parse("2006-01-02 15", timestamp)
		if err != nil {
			return nil, errors.Wrap(err, "Fail to parse timestamp (dt + hour)")
		}
		loc.Timestamp = ts
	}

	// src bucket
	if len(arr) > 5 {
		loc.SrcBucket = arr[5]
	}

	// src key
	if len(arr) > 6 {
		loc.SrcKey = strings.Join(arr[6:], "/")
	}

	return &loc, nil
}

// ToDtKey creates string as partition key
func ToDtKey(ts time.Time) string {
	return "dt=" + ts.Format("2006-01-02")
}

// ToTgKey creates string as partition key
func ToTgKey(tag string) string {
	return "tg=" + tag
}
