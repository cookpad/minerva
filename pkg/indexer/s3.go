package indexer

import (
	"fmt"
	"strings"
)

type s3Loc struct {
	Region string
	Bucket string
	Name   string
	Prefix string
}

func newS3Loc(region, bucket, key string) s3Loc {
	loc := s3Loc{
		Region: region,
		Bucket: bucket,
	}
	loc.SetKey(key)
	return loc
}

func (x s3Loc) Key() string {
	return x.Prefix + x.Name
}

func (x *s3Loc) SetKey(key string) {
	arr := strings.Split(key, "/")
	x.Prefix = strings.Join(arr[:len(arr)-1], "/")
	if x.Prefix != "" {
		x.Prefix = x.Prefix + "/"
	}
	x.Name = arr[len(arr)-1]
}

func (x s3Loc) Path() string {
	return fmt.Sprintf("s3://%s/%s%s", x.Bucket, x.Prefix, x.Name)
}
