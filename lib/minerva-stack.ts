import * as cdk from "@aws-cdk/core";
import * as lambda from "@aws-cdk/aws-lambda";
import * as s3 from "@aws-cdk/aws-s3";
import * as sns from "@aws-cdk/aws-sns";
import * as iam from "@aws-cdk/aws-iam";
import * as dynamodb from "@aws-cdk/aws-dynamodb";
import * as sqs from "@aws-cdk/aws-sqs";
import * as glue from "@aws-cdk/aws-glue";
import * as apigateway from "@aws-cdk/aws-apigateway";

import { SqsSubscription } from "@aws-cdk/aws-sns-subscriptions";
import { SqsEventSource } from "@aws-cdk/aws-lambda-event-sources";

const eventTargets = require("@aws-cdk/aws-events-targets");
// const events = require("@aws-cdk/aws-events");
import * as events from "@aws-cdk/aws-events";

interface MinervaArguments {
  lambdaRoleARN: string;
  dataS3Region: string;
  dataS3Bucket: string;
  dataS3Prefix: string;
  dataSNSTopicARN: string;
  athenaDatabaseName: string;
  metaTableName?: string;
  sentryDSN?: string;
  sentryEnv?: string;
  logLevel?: string;
  concurrentExecution?: number;
  running?: boolean;
}

function getMetaTable(scope: cdk.Construct, metaTableName?: string) {
  const id = "metaTable";
  if (typeof metaTableName === "string") {
    return dynamodb.Table.fromTableName(scope, id, metaTableName);
  } else {
    return new dynamodb.Table(scope, id, {
      partitionKey: { name: "pk", type: dynamodb.AttributeType.STRING },
      sortKey: { name: "sk", type: dynamodb.AttributeType.STRING },
      timeToLiveAttribute: "expires_at",
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
    });
  }
}

export class MinervaStack extends cdk.Stack {
  indexerQueue: sqs.Queue;
  partitionQueue: sqs.Queue;
  mergeQueue: sqs.Queue;
  indexer: lambda.Function;
  merger: lambda.Function;
  partitioner: lambda.Function;
  metaTable: dynamodb.ITable;

  constructor(
    scope: cdk.Construct,
    id: string,
    args: MinervaArguments,
    props?: cdk.StackProps
  ) {
    super(scope, id, props);

    const lambdaRole = iam.Role.fromRoleArn(
      this,
      "LambdaRole",
      args.lambdaRoleARN,
      {
        mutable: false,
      }
    );
    const dataBucket = s3.Bucket.fromBucketArn(
      this,
      "dataBucket",
      "arn:aws:s3:::" + args.dataS3Bucket
    );
    const dataTopic = sns.Topic.fromTopicArn(
      this,
      "dataTopic",
      args.dataSNSTopicARN
    );

    const buildPath = lambda.Code.asset("./build");
    const indexTableName = "indices";
    const messageTableName = "messages";
    const running = args.running || true;

    // DynamoDB
    this.metaTable = getMetaTable(this, args.metaTableName);

    // SQS
    this.indexerQueue = new sqs.Queue(this, "indexerQueue", {
      visibilityTimeout: cdk.Duration.seconds(600),
    });
    dataTopic.addSubscription(new SqsSubscription(this.indexerQueue));

    this.mergeQueue = new sqs.Queue(this, "mergeQueue", {
      visibilityTimeout: cdk.Duration.seconds(450),
    });
    this.partitionQueue = new sqs.Queue(this, "partitionQueue");

    // Lambda Functions
    this.indexer = new lambda.Function(this, "indexer", {
      runtime: lambda.Runtime.GO_1_X,
      handler: "indexer",
      code: buildPath,
      role: lambdaRole,
      timeout: cdk.Duration.seconds(600),
      memorySize: 2048,
      environment: {
        S3_REGION: args.dataS3Region,
        S3_BUCKET: args.dataS3Bucket,
        S3_PREFIX: args.dataS3Prefix,
        INDEX_TABLE_NAME: indexTableName,
        MESSAGE_TABLE_NAME: messageTableName,
        META_TABLE_NAME: this.metaTable.tableName,
        PARTITION_QUEUE: this.partitionQueue.queueUrl,
        SENTRY_DSN: args.sentryDSN ? args.sentryDSN : "",
        SENTRY_ENVIRONMENT: args.sentryEnv ? args.sentryEnv : "",
      },
      reservedConcurrentExecutions: args.concurrentExecution,
    });
    if (running) {
      this.indexer.addEventSource(
        new SqsEventSource(this.indexerQueue, { batchSize: 1 })
      );
    }

    const listIndexObject = new lambda.Function(this, "listIndexObject", {
      runtime: lambda.Runtime.GO_1_X,
      handler: "listIndexObject",
      code: buildPath,
      role: lambdaRole,
      timeout: cdk.Duration.seconds(300),
      memorySize: 1024,
      environment: {
        S3_REGION: args.dataS3Region,
        S3_BUCKET: args.dataS3Bucket,
        S3_PREFIX: args.dataS3Prefix,
        MERGE_QUEUE: this.mergeQueue.queueUrl,
        SENTRY_DSN: args.sentryDSN ? args.sentryDSN : "",
        SENTRY_ENVIRONMENT: args.sentryEnv ? args.sentryEnv : "",
      },
      reservedConcurrentExecutions: 1,
    });
    new events.Rule(this, "ListIndexEvery10min", {
      schedule: events.Schedule.rate(cdk.Duration.minutes(10)),
      targets: [new eventTargets.LambdaFunction(listIndexObject)],
    });

    this.merger = new lambda.Function(this, "mergeIndexObject", {
      runtime: lambda.Runtime.GO_1_X,
      handler: "mergeIndexObject",
      code: buildPath,
      role: lambdaRole,
      timeout: cdk.Duration.seconds(450),
      memorySize: 2048,
      reservedConcurrentExecutions: args.concurrentExecution,
      events: [new SqsEventSource(this.mergeQueue, { batchSize: 1 })],
      environment: {
        SENTRY_DSN: args.sentryDSN ? args.sentryDSN : "",
        SENTRY_ENVIRONMENT: args.sentryEnv ? args.sentryEnv : "",
      },
    });

    this.partitioner = new lambda.Function(this, "makePartition", {
      runtime: lambda.Runtime.GO_1_X,
      handler: "makePartition",
      code: buildPath,
      role: lambdaRole,
      timeout: cdk.Duration.seconds(30),
      memorySize: 2048,
      environment: {
        ATHENA_DB_NAME: args.athenaDatabaseName,
        OBJECT_TABLE_NAME: indexTableName,
        META_TABLE_NAME: this.metaTable.tableName,
        S3_BUCKET: args.dataS3Bucket,
        S3_PREFIX: args.dataS3Prefix,
        SENTRY_DSN: args.sentryDSN ? args.sentryDSN : "",
        SENTRY_ENVIRONMENT: args.sentryEnv ? args.sentryEnv : "",
      },
      reservedConcurrentExecutions: args.concurrentExecution,
      events: [new SqsEventSource(this.partitionQueue, { batchSize: 1 })],
    });

    const indexDB = new glue.Database(this, "indexDB", {
      databaseName: args.athenaDatabaseName,
    });

    const indexTable = new glue.Table(this, "indexTable", {
      tableName: indexTableName,
      database: indexDB,
      partitionKeys: [{ name: "dt", type: glue.Schema.STRING }],
      columns: [
        { name: "tag", type: glue.Schema.STRING },
        { name: "timestamp", type: glue.Schema.BIG_INT },
        { name: "field", type: glue.Schema.STRING },
        { name: "term", type: glue.Schema.STRING },
        { name: "object_id", type: glue.Schema.BIG_INT },
        { name: "seq", type: glue.Schema.INTEGER },
      ],
      bucket: dataBucket,
      s3Prefix: args.dataS3Prefix + "indices/",
      dataFormat: glue.DataFormat.PARQUET,
    });

    const messageTable = new glue.Table(this, "messageTable", {
      tableName: messageTableName,
      database: indexDB,
      partitionKeys: [{ name: "dt", type: glue.Schema.STRING }],
      columns: [
        { name: "timestamp", type: glue.Schema.BIG_INT },
        { name: "object_id", type: glue.Schema.BIG_INT },
        { name: "seq", type: glue.Schema.INTEGER },
        { name: "message", type: glue.Schema.STRING },
      ],
      bucket: dataBucket,
      s3Prefix: args.dataS3Prefix + "messages/",
      dataFormat: glue.DataFormat.PARQUET,
    });

    // API handler
    const apiHandler = new lambda.Function(this, "apiHandler", {
      runtime: lambda.Runtime.GO_1_X,
      handler: "apiHandler",
      code: buildPath,
      role: lambdaRole,
      timeout: cdk.Duration.seconds(120),
      memorySize: 2048,
      environment: {
        S3_REGION: args.dataS3Region,
        S3_BUCKET: args.dataS3Bucket,
        S3_PREFIX: args.dataS3Prefix,
        ATHENA_DB_NAME: indexDB.databaseName,
        INDEX_TABLE_NAME: indexTableName,
        MESSAGE_TABLE_NAME: messageTableName,
        META_TABLE_NAME: this.metaTable.tableName,
      },
    });

    const api = new apigateway.LambdaRestApi(this, "minervaAPI", {
      handler: apiHandler,
      proxy: false,
      cloudWatchRole: false,
      endpointTypes: [apigateway.EndpointType.PRIVATE],
      policy: new iam.PolicyDocument({
        statements: [
          new iam.PolicyStatement({
            actions: ["execute-api:Invoke"],
            resources: ["execute-api:/*/*"],
            effect: iam.Effect.ALLOW,
            principals: [new iam.AnyPrincipal()],
          }),
        ],
      }),
    });

    const v1 = api.root.addResource("api").addResource("v1");
    const searchAPI = v1.addResource("search");
    const apiOption = {
      apiKeyRequired: true,
    };
    searchAPI.addMethod("POST", undefined, apiOption);

    const searchAPIwithID = searchAPI.addResource("{search_id}");
    searchAPIwithID.addMethod("GET", undefined, apiOption);
    searchAPIwithID.addResource("logs").addMethod("GET", undefined, apiOption);
    searchAPIwithID
      .addResource("timeseries")
      .addMethod("GET", undefined, apiOption);
  }
}
