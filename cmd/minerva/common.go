package main

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudformation"
	"github.com/pkg/errors"
)

type arguments struct {
	StackName string
	Region    string
}

func (x arguments) describeStack() (map[string]*cloudformation.StackResource, error) {
	ssn := session.New(&aws.Config{Region: aws.String(x.Region)})
	client := cloudformation.New(ssn)

	input := &cloudformation.DescribeStackResourcesInput{
		StackName: aws.String(x.StackName),
	}

	output, err := client.DescribeStackResources(input)
	if err != nil {
		return nil, errors.Wrapf(err, "Fail to DescribeStackResources for %v", x.StackName)
	}

	resources := map[string]*cloudformation.StackResource{}
	for i := range output.StackResources {
		rsc := output.StackResources[i]
		resources[*rsc.LogicalResourceId] = rsc
	}

	return resources, nil
}
