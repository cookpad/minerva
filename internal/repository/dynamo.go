package repository

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func isConditionalCheckErr(err error) bool {
	if aerr, ok := err.(awserr.RequestFailure); ok {
		return aerr.Code() == dynamodb.ErrCodeConditionalCheckFailedException
	}
	return false
}

func isResourceNotFoundErr(err error) bool {
	if ae, ok := err.(awserr.RequestFailure); ok {
		return ae.Code() == dynamodb.ErrCodeResourceNotFoundException
	}
	return false
}
