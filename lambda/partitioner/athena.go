package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
	"github.com/m-mizutani/minerva/internal"
	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

func createPartition(region, athenaDB string, p models.PartitionQueue, meta internal.MetaAccessor, output string) error {
	ssn := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
	pkey := p.Location

	if has, err := meta.HeadPartition(pkey); err != nil {
		return err
	} else if has {
		return nil // Nothing to do
	}

	var keys []string
	for k, v := range p.Keys {
		keys = append(keys, fmt.Sprintf("%s='%s'", k, v))
	}
	sql := fmt.Sprintf("ALTER TABLE %s.%s ADD IF NOT EXISTS PARTITION (%s) LOCATION '%s'",
		athenaDB, p.TableName, strings.Join(keys, ", "), pkey)

	athenaClient := athena.New(ssn)
	input := &athena.StartQueryExecutionInput{
		QueryString: aws.String(sql),
		ResultConfiguration: &athena.ResultConfiguration{
			OutputLocation: &output,
		},
	}

	logger.WithField("input", input).Info("Athena Query")

	output1, err := athenaClient.StartQueryExecution(input)
	logger.WithFields(logrus.Fields{
		"err":    err,
		"input":  input,
		"output": output1,
	}).Debug("done")

	if err != nil {
		return errors.Wrap(err, "Fail to execute a partitioning query")
	}

	if os.Getenv("CHECK_QUERY_RESULT") != "" {
		for {
			output2, err := athenaClient.GetQueryExecution(&athena.GetQueryExecutionInput{
				QueryExecutionId: output1.QueryExecutionId,
			})
			if err != nil {
				return errors.Wrap(err, "Fail to get an execution result")
			}

			if *output2.QueryExecution.Status.State == "RUNNING" {
				logger.WithField("output", output2).Debug("Waiting...")
				time.Sleep(time.Second * 3)
				continue
			}

			logger.WithField("output", output2).Debug("done")
			break
		}
	}

	if err := meta.PutPartition(pkey); err != nil {
		return err
	}

	return nil
}
