package models

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// RawObject is converted from original log message, but not merged. File format of the object is not defined and any encoding is acceptable by DumpService. This structure is used to indicate path of S3 and partition for Athena.
type RawObject struct {
	DataSize     int64
	prefix       RawObjectPrefix
	fileNameSalt string
	ext          string
}

// NewRawObject is constrcutor of RawObject. *ext* is extension of the object.
func NewRawObject(prefix *RawObjectPrefix, ext string) *RawObject {
	return &RawObject{
		prefix:       *prefix,
		fileNameSalt: uuid.New().String(),
		ext:          ext,
	}
}

// RawObjectPrefix is basic information of RawObject. Basically RawObject is one-on-one relationship to original log S3 object. However sometimes multiple RawObjects are generated from one original log object because too large object size. (Large object can not be converted to Parquet file because of OOM error.) Then base parameters are independent from RawObject.
type RawObjectPrefix struct {
	schema ParquetSchemaName
	src    S3Object
	base   S3Object
	dtKey  string
}

// NewRawObjectPrefix is constructor of RawObjectPrefix. *base* must has destination S3 bucket and prefix. *src* indicates S3 object of original logs. *ts* is log timestamp to identify partition.
func NewRawObjectPrefix(schema ParquetSchemaName, base, src S3Object, ts time.Time) *RawObjectPrefix {
	return &RawObjectPrefix{
		schema: schema,
		base:   base,
		src:    src,
		dtKey:  ts.Format("2006-01-02-15"),
	}
}

// Key of RawObjectPrefix returns unique key for RawObject
func (x *RawObjectPrefix) Key() string {
	return strings.Join([]string{
		string(x.schema),
		x.dtKey,
		x.src.Bucket,
		x.src.Key,
	}, "/")
}

// Schema is getter of schema
func (x *RawObjectPrefix) Schema() ParquetSchemaName { return x.schema }

// Partition returns a part of path
func (x *RawObject) Partition() string {
	return strings.Join([]string{
		x.TableName(),
		x.PartitionLabel(),
	}, "/")
}

// PartitionPath returns S3 path to top of the partition. The path including s3:// prefix and bucket name.
// e.g.) s3://your-bucket/prefix/indicies/dt=2020-01-02-03/
func (x *RawObject) PartitionPath() string {
	return x.prefix.base.AppendKey(x.Partition() + "/").Path()
}

// PartitionKeys returns map of partition name and value
func (x *RawObject) PartitionKeys() map[string]string {
	return map[string]string{
		"dt": x.prefix.dtKey,
	}
}

// PartitionLabel returns a part of S3 path for Athena partition
func (x *RawObject) PartitionLabel() string {
	return fmt.Sprintf("dt=%s", x.prefix.dtKey)
}

// TableName returns Athena table name as string type
func (x *RawObject) TableName() string {
	switch x.prefix.schema {
	case ParquetSchemaIndex:
		return string(AthenaTableIndex)
	case ParquetSchemaMessage:
		return string(AthenaTableMessage)
	}

	return ""
}

// Schema returns schema name as string type
func (x *RawObject) Schema() string {
	return string(x.prefix.schema)
}

// Object returns
func (x *RawObject) Object() *S3Object {
	additionalKey := strings.Join([]string{
		"raw",
		x.Partition(),
		x.prefix.src.Bucket,
		x.prefix.src.Key,
		fmt.Sprintf("%s.%s", x.fileNameSalt, x.ext),
	}, "/")

	return x.prefix.base.AppendKey(additionalKey)
}
