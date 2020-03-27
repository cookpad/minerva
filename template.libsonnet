{
  build(DataS3Region,
        DataS3Bucket,
        DataS3Prefix='',
        AthenaDatabaseName,
        SnsTopicArnMap,
        IndexerProperty,
        SrcS3Buckets=[],
        LambdaRoleArn='',
        SentryDSN='',
        ConcurrentExecution=5):: {
    local IndexTableName = 'indices',
    local ObjectTableName = 'objects',
    local MessageTableName = 'messages',

    local toBucketResource(bucket) = [
      'arn:aws:s3:::' + bucket,
      'arn:aws:s3:::' + bucket + '/*',
    ],
    local toSqsStatement(topic) = {
      Sid: 'MinervaIndexerSQSPolicy001',
      Effect: 'Allow',
      Principal: '*',
      Action: 'sqs:SendMessage',
      Resource: '${IndexerQueue.Arn}',
      Condition: {
        ArnEquals: { 'aws:SourceArn': topic },
      },
    },
    local toSnsSubscription(key, topic) = {
      Type: 'AWS::SNS::Subscription',
      Properties: {
        Endpoint: { 'Fn::GetAtt': 'IndexerQueue.Arn' },
        Protocol: 'sqs',
        TopicArn: topic,
      },
    },

    local LambdaRole = (
      if LambdaRoleArn != '' then LambdaRoleArn else { 'Fn::GetAtt': 'LambdaRole.Arn' }
    ),
    local endpointName = { 'Fn::Sub': '${AWS::StackName}' },

    local sqsPolicy = {
      Version: '2012-10-17',
      Id: 'MinervaIndexerSQSPolicy',
      Statement: std.map(function(k) toSqsStatement(SnsTopicArnMap[k]), std.objectFields(SnsTopicArnMap)),
    },

    local LambdaRoleTemplate = {
      LambdaRole: {
        Type: 'AWS::IAM::Role',
        Properties: {
          AssumeRolePolicyDocument: {
            Version: '2012-10-17',
            Statement: [
              {
                Effect: 'Allow',
                Principal: { Service: ['lambda.amazonaws.com'] },
                Action: ['sts:AssumeRole'],
              },
            ],
          },
          Path: '/',
          ManagedPolicyArns: [
            'arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole',
          ],
          Policies: [
            {
              PolicyName: 'S3Writable',
              PolicyDocument: {
                Version: '2012-10-17',
                Statement: [
                  {
                    Effect: 'Allow',
                    Action: [
                      // According to https://aws.amazon.com/premiumsupport/knowledge-center/athena-output-bucket-error/
                      's3:GetBucketLocation',
                      's3:GetObject',
                      's3:ListBucket',
                      's3:ListBucketMultipartUploads',
                      's3:ListMultipartUploadParts',
                      's3:AbortMultipartUpload',
                      's3:CreateBucket',
                      's3:PutObject',
                    ],
                    Resource: [
                      'arn:aws:s3:::' + DataS3Bucket,
                      'arn:aws:s3:::' + DataS3Bucket + '/' + DataS3Prefix + '*',
                    ],
                  },
                ],
              },
            },
            {
              PolicyName: 'S3Readable',
              PolicyDocument: {
                Version: '2012-10-17',
                Statement: [
                  {
                    Effect: 'Allow',
                    Action: [
                      's3:GetObject',
                      's3:GetObjectVersion',
                      's3:ListBucket',
                    ],
                    Resource: std.join([], std.map(toBucketResource, SrcS3Buckets)),
                  },
                ],
              },
            },

            {
              PolicyName: 'SQSAccess',
              PolicyDocument: {
                Version: '2012-10-17',
                Statement: [
                  {
                    Effect: 'Allow',
                    Action: [
                      'sqs:SendMessage',
                      'sqs:ReceiveMessage',
                      'sqs:DeleteMessage',
                      'sqs:GetQueueAttributes',
                    ],
                    Resource: [
                      { 'Fn::GetAtt': 'IndexerQueue.Arn' },
                      { 'Fn::GetAtt': 'IndexerDeadLetterQueue.Arn' },
                      { 'Fn::GetAtt': 'GeneralDeadLetterQueue.Arn' },
                      { 'Fn::GetAtt': 'IndexerRetryQueue.Arn' },
                      { 'Fn::GetAtt': 'MergeQueue.Arn' },
                      { 'Fn::GetAtt': 'PartitionQueue.Arn' },
                    ],
                  },
                ],
              },
            },

            {
              PolicyName: 'DynamoDBAccess',
              PolicyDocument: {
                Version: '2012-10-17',
                Statement: [
                  {
                    Effect: 'Allow',
                    Action: [
                      'dynamodb:PutItem',
                      'dynamodb:DeleteItem',
                      'dynamodb:GetItem',
                      'dynamodb:Query',
                      'dynamodb:Scan',
                      'dynamodb:UpdateItem',
                    ],
                    Resource: [
                      { 'Fn::GetAtt': 'MetaTable.Arn' },
                      {
                        'Fn::Sub': [
                          '${TableArn}/index/*',
                          { TableArn: { 'Fn::GetAtt': 'MetaTable.Arn' } },
                        ],
                      },
                    ],
                  },
                ],
              },
            },

            {
              PolicyName: 'QueryExecutable',
              PolicyDocument: {
                Version: '2012-10-17',
                Statement: [
                  {
                    Effect: 'Allow',
                    Action: [
                      'glue:GetDatabase',
                      'glue:UpdateDatabase',
                      'glue:GetTable',
                      'glue:BatchCreatePartition',
                    ],
                    Resource: [
                      { 'Fn::Sub': 'arn:aws:glue:${AWS::Region}:${AWS::AccountId}:database/' + AthenaDatabaseName },
                      { 'Fn::Sub': 'arn:aws:glue:${AWS::Region}:${AWS::AccountId}:table/' + AthenaDatabaseName + '/*' },
                    ],
                  },
                  {
                    Effect: 'Allow',
                    Action: [
                      'glue:GetDatabase',
                      'glue:GetTable',
                      'glue:BatchCreatePartition',
                    ],
                    Resource: [
                      { 'Fn::Sub': 'arn:aws:glue:${AWS::Region}:${AWS::AccountId}:catalog' },
                    ],
                  },
                  {
                    Effect: 'Allow',
                    Action: ['glue:GetDatabase'],
                    Resource: [
                      { 'Fn::Sub': 'arn:aws:glue:${AWS::Region}:${AWS::AccountId}:database/default' },
                    ],
                  },
                  {
                    Effect: 'Allow',
                    Action: [
                      'athena:StartQueryExecution',
                      'athena:GetQueryExecution',
                    ],
                    Resource: ['*'],
                  },
                ],
              },
            },
          ],
        },
      },
    },

    // --- main template ---------------------------------------------
    AWSTemplateFormatVersion: '2010-09-09',
    Transform: 'AWS::Serverless-2016-10-31',

    Resources: {
      Indexer: {
        Type: 'AWS::Serverless::Function',
        Properties: {
          Runtime: 'go1.x',
          Timeout: 600,
          MemorySize: 2048,
          ReservedConcurrentExecutions: ConcurrentExecution,
          Role: LambdaRole,
          Environment: {
            Variables: {
              S3_REGION: DataS3Region,
              S3_BUCKET: DataS3Bucket,
              S3_PREFIX: DataS3Prefix,
              INDEX_TABLE_NAME: IndexTableName,
              MESSAGE_TABLE_NAME: MessageTableName,
              META_TABLE_NAME: { Ref: 'MetaTable' },
              PARTITION_QUEUE: { Ref: 'PartitionQueue' },
              LOG_LEVEL: 'INFO',
              SENTRY_DSN: SentryDSN,
            },
          },
          Events: {
            IndexerQueue: {
              Type: 'SQS',
              Properties: {
                Queue: { 'Fn::GetAtt': 'IndexerQueue.Arn' },
                BatchSize: 1,
              },
            },
          },
        } + IndexerProperty,
      },

      listIndexObject: {
        Type: 'AWS::Serverless::Function',
        Properties: {
          CodeUri: 'build',
          Handler: 'listIndexObject',
          Runtime: 'go1.x',
          Timeout: 300,
          MemorySize: 1024,
          Role: LambdaRole,
          Environment: {
            Variables: {
              S3_REGION: DataS3Region,
              S3_BUCKET: DataS3Bucket,
              S3_PREFIX: DataS3Prefix,
              MERGE_QUEUE: { Ref: 'MergeQueue' },
              LOG_LEVEL: 'DEBUG',
              SENTRY_DSN: SentryDSN,
            },
          },
          DeadLetterQueue: {
            Type: 'SQS',
            TargetArn: { 'Fn::GetAtt': 'GeneralDeadLetterQueue.Arn' },
          },
          Events: {
            Every5mins: {
              Type: 'Schedule',
              Properties: { Schedule: 'rate(10 minutes)' },
            },
          },
        },
      },

      mergeIndexObject: {
        Type: 'AWS::Serverless::Function',
        Properties: {
          CodeUri: 'build',
          Handler: 'mergeIndexObject',
          Runtime: 'go1.x',
          Timeout: 450,
          MemorySize: 2048,
          Role: LambdaRole,
          ReservedConcurrentExecutions: 20,
          Environment: {
            Variables: {
              LOG_LEVEL: 'DEBUG',
              SENTRY_DSN: SentryDSN,
            },
          },
          DeadLetterQueue: {
            Type: 'SQS',
            TargetArn: { 'Fn::GetAtt': 'GeneralDeadLetterQueue.Arn' },
          },
          Events: {
            MergeJob: {
              Type: 'SQS',
              Properties: {
                Queue: { 'Fn::GetAtt': 'MergeQueue.Arn' },
                BatchSize: 1,
              },
            },
          },
        },
      },

      makePartition: {
        Type: 'AWS::Serverless::Function',
        Properties: {
          CodeUri: 'build',
          Handler: 'makePartition',
          Runtime: 'go1.x',
          Timeout: 30,
          MemorySize: 128,
          Role: LambdaRole,
          Environment: {
            Variables: {
              ATHENA_DB_NAME: AthenaDatabaseName,
              OBJECT_TABLE_NAME: IndexTableName,
              META_TABLE_NAME: { Ref: 'MetaTable' },
              S3_BUCKET: DataS3Bucket,
              S3_PREFIX: DataS3Prefix,
              LOG_LEVEL: 'INFO',
              SENTRY_DSN: SentryDSN,
            },
          },
          DeadLetterQueue: {
            Type: 'SQS',
            TargetArn: { 'Fn::GetAtt': 'GeneralDeadLetterQueue.Arn' },
          },
          Events: {
            PartitionJob: {
              Type: 'SQS',
              Properties: {
                Queue: { 'Fn::GetAtt': 'PartitionQueue.Arn' },
                BatchSize: 10,
              },
            },
          },
        },
      },

      errorHandler: {
        Type: 'AWS::Serverless::Function',
        Properties: {
          CodeUri: 'build',
          Handler: 'errorHandler',
          Runtime: 'go1.x',
          Timeout: 30,
          MemorySize: 128,
          Role: LambdaRole,
          Environment: {
            Variables: {
              GENERAL_DLQ: { 'Fn::GetAtt': 'GeneralDeadLetterQueue.Arn' },
              INDEXER_DLQ: { 'Fn::GetAtt': 'IndexerDeadLetterQueue.Arn' },
              RETRY_QUEUE: { Ref: 'IndexerRetryQueue' },
              SENTRY_DSN: SentryDSN,
            },
          },
          Events: {
            GeneralDLQ: {
              Type: 'SQS',
              Properties: {
                Queue: { 'Fn::GetAtt': 'GeneralDeadLetterQueue.Arn' },
                BatchSize: 10,
              },
            },
            IndexerDLQ: {
              Type: 'SQS',
              Properties: {
                Queue: { 'Fn::GetAtt': 'IndexerDeadLetterQueue.Arn' },
                BatchSize: 10,
              },
            },
          },
        },
      },

      LoadIndexerRetry: {
        Type: 'AWS::Serverless::Function',
        Properties: {
          CodeUri: 'build',
          Handler: 'loadIndexerRetry',
          Runtime: 'go1.x',
          Timeout: 300,
          MemorySize: 128,
          Role: LambdaRole,
          Environment: {
            Variables: {
              RETRY_QUEUE: { Ref: 'IndexerRetryQueue' },
              INDEXER_QUEUE: { Ref: 'IndexerQueue' },
              LOG_LEVEL: 'DEBUG',
              SENTRY_DSN: SentryDSN,
            },
          },
        },
      },

      // Partition Tables
      MetaTable: {
        Type: 'AWS::DynamoDB::Table',
        Properties: {
          AttributeDefinitions: [
            {
              AttributeName: 'pk',
              AttributeType: 'S',
            },
          ],
          KeySchema: [
            {
              AttributeName: 'pk',
              KeyType: 'HASH',
            },
          ],
          ProvisionedThroughput: {
            ReadCapacityUnits: 20,
            WriteCapacityUnits: 20,
          },
          TimeToLiveSpecification: {
            AttributeName: 'expires_at',
            Enabled: true,
          },
        },
      },

      SearchTable: {
        Type: 'AWS::DynamoDB::Table',
        Properties: {
          AttributeDefinitions: [
            {
              AttributeName: 'pk',
              AttributeType: 'S',
            },
            {
              AttributeName: 'sk',
              AttributeType: 'S',
            },
          ],
          KeySchema: [
            {
              AttributeName: 'pk',
              KeyType: 'HASH',
            },
            {
              AttributeName: 'sk',
              KeyType: 'RANGE',
            },
          ],
          BillingMode: 'PAY_PER_REQUEST',
          TimeToLiveSpecification: {
            AttributeName: 'expires_at',
            Enabled: true,
          },
        },
      },

      // SQS
      IndexerQueue: {
        Type: 'AWS::SQS::Queue',
        Properties: {
          VisibilityTimeout: 600,
          RedrivePolicy: {
            deadLetterTargetArn: { 'Fn::GetAtt': 'IndexerDeadLetterQueue.Arn' },
            maxReceiveCount: 5,
          },
        },
      },
      IndexerQueuePolicy: {
        Type: 'AWS::SQS::QueuePolicy',
        Properties: {
          PolicyDocument: { 'Fn::Sub': std.toString(sqsPolicy) },
          Queues: [{ Ref: 'IndexerQueue' }],
        },
      },

      MergeQueue: {
        Type: 'AWS::SQS::Queue',
        Properties: {
          VisibilityTimeout: 450,
          RedrivePolicy: {
            deadLetterTargetArn: { 'Fn::GetAtt': 'GeneralDeadLetterQueue.Arn' },
            maxReceiveCount: 5,
          },
        },
      },
      PartitionQueue: {
        Type: 'AWS::SQS::Queue',
      },
      IndexerDeadLetterQueue: {
        Type: 'AWS::SQS::Queue',
      },
      GeneralDeadLetterQueue: {
        Type: 'AWS::SQS::Queue',
      },
      IndexerRetryQueue: {
        Type: 'AWS::SQS::Queue',
      },

      IndexDB: {
        Type: 'AWS::Glue::Database',
        Properties: {
          CatalogId: { Ref: 'AWS::AccountId' },
          DatabaseInput: {
            Description: 'Log Index Database',
            Name: AthenaDatabaseName,
          },
        },
      },

      IndexTable: {
        Type: 'AWS::Glue::Table',
        Properties: {
          TableInput: {
            Description: 'Inverted index table',
            TableType: 'EXTERNAL_TABLE',
            PartitionKeys: [
              { Name: 'dt', Type: 'string' },
            ],
            StorageDescriptor: {
              InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
              Columns: [
                { Name: 'tag', Type: 'string' },
                { Name: 'timestamp', Type: 'bigint' },
                { Name: 'field', Type: 'string' },
                { Name: 'term', Type: 'string' },
                { Name: 'object_id', Type: 'bigint' },
                { Name: 'seq', Type: 'int' },
              ],
              SerdeInfo: {
                SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
              },
              Location: 's3://' + DataS3Bucket + '/' + DataS3Prefix + 'indices/',
            },
            Name: IndexTableName,
          },
          DatabaseName: { Ref: 'IndexDB' },
          CatalogId: { Ref: 'AWS::AccountId' },
        },
      },

      MessageTable: {
        Type: 'AWS::Glue::Table',
        Properties: {
          TableInput: {
            Description: 'Log message table',
            TableType: 'EXTERNAL_TABLE',
            PartitionKeys: [
              { Name: 'dt', Type: 'string' },
            ],
            StorageDescriptor: {
              InputFormat: 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
              Columns: [
                { Name: 'timestamp', Type: 'bigint' },
                { Name: 'object_id', Type: 'bigint' },
                { Name: 'seq', Type: 'int' },
                { Name: 'message', Type: 'string' },
              ],
              SerdeInfo: {
                SerializationLibrary: 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
              },
              Location: 's3://' + DataS3Bucket + '/' + DataS3Prefix + 'messages/',
            },
            Name: MessageTableName,
          },
          DatabaseName: { Ref: 'IndexDB' },
          CatalogId: { Ref: 'AWS::AccountId' },
        },
      },

      // ====================== API =======================
      ApiHandler: {
        Type: 'AWS::Serverless::Function',
        Properties: {
          CodeUri: 'build',
          Handler: 'apiHandler',
          Runtime: 'go1.x',
          Timeout: 120,
          MemorySize: 2048,
          Role: LambdaRole,
          Environment: {
            Variables: {
              ATHENA_DB_NAME: AthenaDatabaseName,
              INDEX_TABLE_NAME: IndexTableName,
              MESSAGE_TABLE_NAME: MessageTableName,
              S3_BUCKET: DataS3Bucket,
              S3_PREFIX: DataS3Prefix,
              LOG_LEVEL: 'DEBUG',
              SENTRY_DSN: SentryDSN,
              SEARCH_TABLE_NAME: { Ref: 'SearchTable' },
            },
          },
          Events: {
            PostSearch: {
              Type: 'Api',
              Properties: {
                Method: 'post',
                Path: '/api/v1/search',
                RestApiId: { Ref: 'ApiGW' },
                Auth: { ApiKeyRequired: true },
              },
            },
            GetSearch: {
              Type: 'Api',
              Properties: {
                Method: 'get',
                Path: '/api/v1/search/{search_id}',
                RestApiId: { Ref: 'ApiGW' },
                Auth: { ApiKeyRequired: true },
              },
            },
            GetSearchLogs: {
              Type: 'Api',
              Properties: {
                Method: 'get',
                Path: '/api/v1/search/{search_id}/logs',
                RestApiId: { Ref: 'ApiGW' },
                Auth: { ApiKeyRequired: true },
              },
            },
            GetSearchTimeline: {
              Type: 'Api',
              Properties: {
                Method: 'get',
                Path: '/api/v1/search/{search_id}/timeseries',
                RestApiId: { Ref: 'ApiGW' },
                Auth: { ApiKeyRequired: true },
              },
            },
          },
        },
      },

      ApiGW: {
        Type: 'AWS::Serverless::Api',
        Properties: {
          StageName: 'prod',
          EndpointConfiguration: 'PRIVATE',
          Auth: {
            ResourcePolicy: {
              CustomStatements: [
                {
                  Effect: 'Allow',
                  Principal: '*',
                  Action: 'execute-api:Invoke',
                  Resource: 'execute-api:/*/*',
                },
              ],
            },
          },
        },
      },
    } + std.mapWithKey(toSnsSubscription, SnsTopicArnMap) + (if LambdaRoleArn == '' then LambdaRoleTemplate else {}),
  },
}
