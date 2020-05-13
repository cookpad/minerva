#!/usr/bin/env node
import "source-map-support/register";
import * as cdk from "@aws-cdk/core";
import { MinervaStack } from "../lib/minerva-stack";

const app = new cdk.App();

new MinervaStack(app, "MyMinervaStack", {
  dataS3Region: process.env.MINERVA_S3_REGION!,
  dataS3Bucket: process.env.MINERVA_S3_BUCKET!,
  dataS3Prefix: process.env.MINERVA_S3_PREFIX!,
  athenaDatabaseName: process.env.MINERVA_ATHENA_DB_NAME!,
  dataSNSTopicARN: process.env.MINERVA_SNS_TOPIC_ARN!,
  lambdaRoleARN: process.env.MINERVA_LAMBDA_ROLE_ARN!,
});
