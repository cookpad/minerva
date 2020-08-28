package models

// Record is interface of Index and Message records
type Record interface{}

// IndexRecord is used for inverted index of log files on S3 bucket.
type IndexRecord struct {
	Tag string `parquet:"name=tag, type=UTF8, encoding=PLAIN_DICTIONARY" json:"tag" msgpack:"tag"`
	// Timestamp is unixtime (second) of original log.
	Timestamp int64  `parquet:"name=timestamp, type=INT64" json:"timestamp" msgpack:"timestamp"`
	Field     string `parquet:"name=field, type=UTF8, encoding=PLAIN_DICTIONARY" json:"field" msgpack:"field"`
	Term      string `parquet:"name=term, type=UTF8, encoding=PLAIN_DICTIONARY" json:"term" msgpack:"term"`
	ObjectID  int64  `parquet:"name=object_id, type=INT64" json:"object_id" msgpack:"object_id"`
	Seq       int32  `parquet:"name=seq, type=INT32" json:"seq" msgpack:"seq"`
}

// MessageRecord stores original log message that is encoded to JSON.
type MessageRecord struct {
	// Timestamp is unixtime (second) of original log.
	Timestamp int64  `parquet:"name=timestamp, type=INT64" json:"timestamp" msgpack:"timestamp"`
	ObjectID  int64  `parquet:"name=object_id, type=INT64" json:"object_id" msgpack:"object_id"`
	Seq       int32  `parquet:"name=seq, type=INT32" json:"seq" msgpack:"seq"`
	Message   string `parquet:"name=message, type=UTF8, encoding=PLAIN_DICTIONARY" json:"message" msgpack:"message"`
}
