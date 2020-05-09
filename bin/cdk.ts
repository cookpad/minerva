#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "@aws-cdk/core";
import { MinervaStack } from "../lib/minerva-stack";
import * as s3 from "@aws-cdk/aws-s3";
import * as sns from "@aws-cdk/aws-sns";
import * as iam from "@aws-cdk/aws-iam";

const app = new cdk.App();

const bucket = s3.Bucket.fromBucketArn(
  app,
  "dataBucket",
  "arn:aws:s3:::" + process.env.MINERVA_S3_BUCKET
);
const topic = sns.Topic.fromTopicArn(
  app,
  "dataTopic",
  process.env.MINERVA_SNS_TOPIC_ARN!
);
const lambdaRole = iam.Role.fromRoleArn(
  app,
  "LambdaRole",
  process.env.MINERVA_LAMBDA_ROLE_ARN!,
  { mutable: false }
);

new MinervaStack(app, "MyMinervaStack", {
  dataS3Bucket: bucket,
  dataS3Prefix: process.env.MINERVA_S3_PREFIX!,
  athenaDatabaseName: process.env.MINERVA_ATHENA_DB_NAME!,
  dataSNSTopic: topic,
  lambdaRole: lambdaRole,
});
