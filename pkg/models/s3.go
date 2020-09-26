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

type S3Objects struct {
	objects []*S3Object `json:"-"`
	Raw     string      `json:"raw"`
}

func NewS3Objects(objects []*S3Object) (*S3Objects, error) {
	s3obj := &S3Objects{}
	if err := s3obj.Append(objects...); err != nil {
		return nil, errors.Wrap(err, "Failed to create new S3Objects")
	}
	return s3obj, nil
}

func (x *S3Objects) Append(objects ...*S3Object) error {
	x.objects = append(x.objects, objects...)
	raw, err := encodeS3Objects(x.objects)
	if err != nil {
		return err
	}
	x.Raw = string(raw)
	return nil
}

func (x *S3Objects) Export() ([]*S3Object, error) {
	objects, err := decodeS3Objects([]byte(x.Raw))
	if err != nil {
		return nil, err
	}
	x.objects = objects
	return x.objects, nil
}

func encodeS3Objects(objects []*S3Object) ([]byte, error) {
	raw, err := json.Marshal(objects)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to marshal []*S3Object")
	}

	buf := &bytes.Buffer{}
	base64Writer := base64.NewEncoder(base64.RawStdEncoding, buf)
	gzipWriter := gzip.NewWriter(base64Writer)

	if _, err := gzipWriter.Write(raw); err != nil {
		return nil, errors.Wrap(err, "Failed to encode []*S3Objects to base64")
	}

	if err := gzipWriter.Close(); err != nil {
		return nil, errors.Wrap(err, "Failed to close gzip writer for []*S3Objects")
	}
	if err := base64Writer.Close(); err != nil {
		return nil, errors.Wrap(err, "Failed to close base64 writer for []*S3Objects")
	}
	return buf.Bytes(), nil
}

func decodeS3Objects(raw []byte) ([]*S3Object, error) {
	var objects []*S3Object
	buf := bytes.NewReader(raw)
	base64Reader := base64.NewDecoder(base64.RawStdEncoding, buf)

	gzipReader, err := gzip.NewReader(base64Reader)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create gzip.Reader for []*S3Object")
	}

	decoded, err := ioutil.ReadAll(gzipReader)
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
