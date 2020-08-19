package models

import "github.com/m-mizutani/minerva/internal"

// ComposeQueue is sent by indexer and received by composer
type ComposeQueue struct {
	S3Object  internal.S3Object `json:"s3_object"`
	Size      int64             `json:"size"`
	Schema    string            `json:"schema"`
	Partition string            `json:"partition"`
}
