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
  "arn:aws:s3:::my-bucket"
);
const topic = sns.Topic.fromTopicArn(
  app,
  "dataTopic",
  "arn:aws:sns:ap-northeast-1:1234567890:mytopic"
);
const lambdaRole = iam.Role.fromRoleArn(
  app,
  "LambdaRole",
  "arn:aws:iam::1234567890:role/LambdaMinervaRole",
  {
    mutable: false,
  }
);

new MinervaStack(app, "MyMinervaStack", {
  dataS3Bucket: bucket,
  dataS3Prefix: "testing",
  athenaDatabaseName: "test",
  dataSNSTopic: topic,
  lambdaRole: lambdaRole,
});
