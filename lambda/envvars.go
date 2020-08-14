package lambda

// EnvVars has all environment variables that should be given to Lambda function
type EnvVars struct {
	// From arguments
	AthenaDBName     string `env:"ATHENA_DB_NAME"`
	S3Region         string `env:"S3_REGION"`
	S3Bucket         string `env:"S3_BUCKET"`
	S3Prefix         string `env:"S3_PREFIX"`
	IndexTableName   string `env:"INDEX_TABLE_NAME"`
	MessageTableName string `env:"MESSAGE_TABLE_NAME"`
	SentryDSN        string `env:"SENTRY_DSN"`
	SentryEnv        string `env:"SENTRY_ENVIRONMENT"`
	LogLevel         string `env:"LOG_LEVEL"`

	// From resource
	MetaTableName     string `env:"META_TABLE_NAME"`
	PartitionQueueURL string `env:"PARTITION_QUEUE_URL"`
	ComposeQueueURL   string `env:"COMPOSE_QUEUE_URL"`
	MergeQueueURL     string `env:"MERGE_QUEUE_URL"`

	// From AWS Lambda
	AwsRegion string `env:"AWS_REGION"`
}
