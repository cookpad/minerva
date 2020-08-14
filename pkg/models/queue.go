package models

// ComposeQueue is sent by indexer and received by composer
type ComposeQueue struct {
	S3Region  string `json:"s3_region"`
	S3Bucket  string `json:"s3_bucket"`
	S3Key     string `json:"s3_key"`
	Size      int    `json:"size"`
	Type      string `json:"type"`
	Partition string `json:"partition"`
}
