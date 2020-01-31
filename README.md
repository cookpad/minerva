# minerva [![Build Status](https://travis-ci.org/m-mizutani/minerva.svg?branch=master)](https://travis-ci.org/m-mizutani/minerva)  [![Report card](https://goreportcard.com/badge/github.com/m-mizutani/minerva)](https://goreportcard.com/report/github.com/m-mizutani/minerva)

Serverless Log Search Architecture for Security Monitoring based on Amazon Athena.

## Overview

In security monitoring, a security engineer is required to analyze security alert from security devices to determine risk of the alert. When analyzing a security alert, various logs from system, application, middleware, network and 3rd party services strongly help a security engineer to understand what is happened around the alert.
There are a lot of existing useful log search engine products and services. However these products and services are expensive due to amount of log traffic size.

Minerva is designed focusing on cost effectiveness by leveraging AWS managed serviecs. Target use case is log search for several security alerts per day.

- Advantages
  - **Low running cost**: (e.g. 7.5 TB logs and several searches per day require about only $300/mo as total)
  - **Low operational cost**: All components of Minerva are managed services and require minimum operation. Additionally preprocessing Lambda function can smoothly scale in/out.
- Disadvantage
  - Cost increases accourding to number of search times. Then Minerva is not appropriate for continuous searching operation (e.g. Threat hunting).
  - Amazon Athena has latency in search operation about from 10 seconds to several minutes. This latency is bigger than other search engines (e.g. Elasticsearch).

Minerva provides only API to saerch logs. See [Strix](https://github.com/m-mizutani/strix) as web based user interface for Minerva. A following figure shows abstracted architecture of Minerva and Strix.

![rough arch](https://user-images.githubusercontent.com/605953/73524333-3bd35700-4450-11ea-92de-ac05d077dafd.png)

On a side note, Minerva is the Roman goddess that is equated with Athena.

## Getting Started

### Prerequisite

- Tools
  - aws-cli >= 1.16.310
  - go >= 1.13
  - jsonnet >= 0.14.0
- Resources
  - S3 bucket stored logs (assuming bucket name is `s3-log-bucket`)
  - S3 bucket stored parquet files (assuming bucket name is `s3-parquet-bucket`)
  - Amazon SNS receiving `s3:ObjectCreated`. See [docs](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html) to configure. (assuming topic name is `s3-log-create-topic`)

### Configurations

3 configuration files are required.

- `stack.jsonnet`: Describe CloudFormation stack settings.
- `sam.jsonnet`: Describe parameters for CloudFormatoin template.
- `indexer.go` Describe binding log format and S3 bucket stored original log objects.

Example: `stack.jsonnet`

```jsonnet
{
  StackName: 'minerva',
  CodeS3Bucket: 'some-s3-bucket', // Storing Lambda code
  CodeS3Prefix: 'functions', // Optional
  Region: 'ap-northeast-1',
}
```

Example: `sam.jsonnet`

```jsonnet
local template = import 'template.libsonnet';

local indexer = {
  CodeUri: 'build',
  Handler: 'indexer',
};

// You do not need to modify above lines basically.

template.build(
  DataS3Region='ap-northeast-1', // region where you want to deploy to
  DataS3Bucket='s3-parquet-bucket', // in list of prerequisite resources
  AthenaDatabaseName='minerva_db',
  SrcS3Buckets=['s3-log-bucket'], // in list of prerequisite resources
  SnsTopicArn='arn:aws:sns:ap-northeast-1:12345xxxxxx:s3-log-create-topic', // in list of prerequisite resources
  IndexerProperty=indexer
)
```

Example: `indexer.go`

```go
package main

import (
	"context"

	"github.com/m-mizutani/rlogs"
	"github.com/m-mizutani/rlogs/parser"
	"github.com/m-mizutani/rlogs/pipeline"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/m-mizutani/minerva/pkg/indexer"
)

func main() {
	lambda.Start(func(ctx context.Context, event events.SQSEvent) error {
		logEntries := []*rlogs.LogEntry{
			// VPC FlowLogs
			{
				Pipe: pipeline.NewVpcFlowLogs(),
				Src: &rlogs.AwsS3LogSource{
					Region: "ap-northeast-1",
					Bucket: "my-flow-logs",
					Key:    "AWSLogs/",
				},
			},

			// Syslog
			{
				Pipe: rlogs.Pipeline{
					Ldr: &rlogs.S3LineLoader{},
					Psr: &parser.JSON{
						Tag:             "ec2.syslog",
						TimestampField:  rlogs.String("timestamp"),
						TimestampFormat: rlogs.String("2006-01-02T15:04:05-0700"),
					},
				},
				Src: &rlogs.AwsS3LogSource{
					Region: "ap-northeast-1",
					Bucket: "my-ec2-syslog",
					Key:    "logs/",
				},
			},
		}

		return indexer.RunIndexer(ctx, event, rlogs.NewReader(logEntries))
	})
}
```

`indexer.go` is written based on [rlogs](https://github.com/m-mizutani/rlogs). Please see the repository for more detail.

### Deployment

```bash
$ ls
indexer.go
sam.jsonnet
stack.jsonnet
$ go mod init indexer
$ env GOARCH=amd64 GOOS=linux go build -o build/indexer .
$ make # build all binaries and deploy CloudFormation
```

After succeeding deployment, you can access Minerva via HTTPS API from **inside of VPC network that has API gateway endpoint**.

```
% export API_ID=`cat output.json | jq '.StackResources[] | select
(.LogicalResourceId == "ApiGW") | .PhysicalResourceId ' -r`
% export AWS_REGION=ap-northeast-1
% curl -X POST "https://$API_ID.execute-api.ap-northeast-1.amazonaws.com/prod/api/v1/search" -d '{"terms":["words", "you", "wanna", "search"]}'
```

## Development

### Architecture Overview

![github-readme](https://user-images.githubusercontent.com/605953/73502265-baf26c00-440b-11ea-8e95-59e22c69dae6.png)

## License

MIT License
