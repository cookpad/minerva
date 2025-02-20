> [!WARNING]  
> This repo is now (as of 2025-02-20) archived and will not receive future updates, security related or otherwise. Use at your own risk.

# minerva 

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
  - aws-cdk >= 1.38.0
  - go >= 1.13
- Resources
  - S3 bucket stored logs (assuming bucket name is `s3-log-bucket`)
  - S3 bucket stored parquet files (assuming bucket name is `s3-parquet-bucket`)
  - Amazon SNS receiving `s3:ObjectCreated`. See [docs](https://docs.aws.amazon.com/AmazonS3/latest/dev/NotificationHowTo.html) to configure. (assuming topic name is `s3-log-create-topic`)
  - IAM role for Lambda Function to access S3 bucket and so on. (assuming role name is `YourLambdaRole` )
  - Additionally, these resources are in `ap-northeast-1` region and account ID is `1234567890x`

### Configurations

Init your configuration directry by `cdk init` command.

```sh
$ cdk init --language typescript
```

Then, update `bin/cdk.ts` like following.

```ts
#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "@aws-cdk/core";
import { MinervaStack } from "../minerva/lib/minerva-stack";

const app = new cdk.App();
const stackID = "your-stack-name";
new MinervaStack(
  app,
  stackID,
  {
    dataS3Region: "ap-northeast-1",
    dataS3Bucket: "s3-parquet-bucket",
    dataS3Prefix: "production/", // Set as you like it
    athenaDatabaseName: "minerva_db", // Set as you like it
    dataSNSTopicARN:
      "arn:aws:sns:ap-northeast-1:1234567890x:s3-log-create-topic",
    lambdaRoleARN: "arn:aws:iam::1234567890x:role/YourLambdaRole",
  },
  {
    stackName: stackID,
    env: {
      region: "ap-northeast-1",
      account: "1234567890x",
    },
  }
);
```

After that, create `indexer.go`. An example is following.

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

Lastly, clone minerva repository.

```sh
$ git clone git@github.com:m-mizutani/minerva.git
```

### Deployment

```bash
$ go mod init indexer
$ env GOARCH=amd64 GOOS=linux go build -o build/indexer .
$ npm install
$ npm run build
$ cdk deploy
```

## Development

### Architecture Overview

![github-readme](https://user-images.githubusercontent.com/605953/73502265-baf26c00-440b-11ea-8e95-59e22c69dae6.png)

## License

MIT License
