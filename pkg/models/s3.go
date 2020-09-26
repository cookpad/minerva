package models

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/klauspost/compress/gzip"
	"github.com/pkg/errors"
)

type S3Object struct {
	Region string `json:"region"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

func EncodeS3Objects(objects []*S3Object) ([]byte, error) {
	raw, err := json.Marshal(objects)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to marshal []*S3Object")
	}

	buf := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buf)
	base64Writer := base64.NewEncoder(base64.RawStdEncoding, gzipWriter)

	if _, err := base64Writer.Write(raw); err != nil {
		return nil, errors.Wrap(err, "Failed to encode []*S3Objects to base64")
	}

	if err := base64Writer.Close(); err != nil {
		return nil, errors.Wrap(err, "Failed to close base64 writer for []*S3Objects")
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, errors.Wrap(err, "Failed to close gzip writer for []*S3Objects")
	}

	return buf.Bytes(), nil
}

func DecodeS3Objects(raw []byte) ([]*S3Object, error) {
	var objects []*S3Object

	gzipReader, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create gzip.Reader for []*S3Object")
	}
	base64Reader := base64.NewDecoder(base64.RawStdEncoding, gzipReader)

	decoded, err := ioutil.ReadAll(base64Reader)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read raw data for []*S3Object")
	}

	if err := json.Unmarshal(decoded, &objects); err != nil {
		return nil, errors.Wrap(err, "Failed to unmarshal []*S3Object")
	}

	return objects, nil
}

func NewS3Object(region, bucket, key string) S3Object {
	return S3Object{
		Region: region,
		Bucket: bucket,
		Key:    key,
	}
}

func NewS3ObjectFromRecord(record events.S3EventRecord) S3Object {
	return S3Object{
		Region: record.AWSRegion,
		Bucket: record.S3.Bucket.Name,
		Key:    record.S3.Object.Key,
	}
}

func (x *S3Object) AppendKey(postfix string) *S3Object {
	newObj := *x
	newObj.Key += postfix
	return &newObj
}

func (x *S3Object) Encode() string {
	return fmt.Sprintf("%s@%s:%s", x.Bucket, x.Region, x.Key)
}

// Path returns full path by s3 bucket name and key.
// e.g.) s3://your-bucket/some/key
func (x *S3Object) Path() string {
	return fmt.Sprintf("s3://%s/%s", x.Bucket, x.Key)
}

func DecodeS3Object(raw string) (*S3Object, error) {
	p1 := strings.Split(raw, "@")
	if len(p1) != 2 {
		return nil, errors.New("Invalid S3 path encode (@ is required)")
	}

	p2 := strings.Split(p1[1], ":")
	if len(p2) < 2 {
		return nil, errors.New("Invalid S3 path encode (: is required)")
	}

	return &S3Object{
		Bucket: p1[0],
		Region: p2[0],
		Key:    strings.Join(p2[1:], ":"),
	}, nil
}
