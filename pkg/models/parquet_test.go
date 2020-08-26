package models_test

import (
	"log"
	"testing"
	"time"

	"github.com/m-mizutani/minerva/pkg/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newGenericParquetLocation() models.ParquetLocation {
	ts, err := time.Parse("2006-01-02 15", "1983-04-20 13")
	if err != nil {
		log.Fatal(err)
	}

	pqLoc := models.ParquetLocation{
		Bucket:    "mybucket",
		MergeStat: models.ParquetMergeStatUnmerged,
		Timestamp: ts,
		Schema:    models.ParquetSchemaIndex,
		SrcBucket: "srcbucket",
		SrcKey:    "srckey",
	}

	return pqLoc
}

func TestParquetLocationS3Key(t *testing.T) {
	pqLoc := newGenericParquetLocation()

	assert.Equal(t, "raw/indices/dt=1983-04-20-13/srcbucket/srckey.parquet", pqLoc.S3Key())

	pqLoc.Prefix = "myprefix/"
	assert.Equal(t, "myprefix/raw/indices/dt=1983-04-20-13/srcbucket/srckey.parquet", pqLoc.S3Key())

	pqLoc.Schema = models.ParquetSchemaMessage
	assert.Equal(t, "myprefix/raw/messages/dt=1983-04-20-13/srcbucket/srckey.parquet", pqLoc.S3Key())
}

func TestParquetLocationTableName(t *testing.T) {
	pqLoc := newGenericParquetLocation()

	pqLoc.Schema = models.ParquetSchemaIndex
	assert.Equal(t, "indices", pqLoc.TableName())

	pqLoc.Schema = models.ParquetSchemaMessage
	assert.Equal(t, "messages", pqLoc.TableName())
}

func TestParquetLocationPertitionPrefix(t *testing.T) {
	pqLoc := newGenericParquetLocation()
	assert.Equal(t, "indices/dt=1983-04-20-13", pqLoc.PartitionPrefix())
}

func TestParquetLocationPertitionSchemaPrefix(t *testing.T) {
	pqLoc := newGenericParquetLocation()
	assert.Equal(t, "indices", pqLoc.PartitionSchemaPrefix())
}

func TestParquetLocationPartitionLocation(t *testing.T) {
	pqLoc := newGenericParquetLocation()
	assert.Equal(t, "s3://mybucket/indices/dt=1983-04-20-13/", pqLoc.PartitionLocation())

	pqLoc.Prefix = "myprefix/"
	assert.Equal(t, "s3://mybucket/myprefix/indices/dt=1983-04-20-13/", pqLoc.PartitionLocation())

	pqLoc.Prefix = "myprefix"
	assert.Equal(t, "s3://mybucket/myprefixindices/dt=1983-04-20-13/", pqLoc.PartitionLocation())
}

func TestParquetLocationPartitionKeys(t *testing.T) {
	pqLoc := newGenericParquetLocation()
	keys := pqLoc.PartitionKeys()

	dt, ok1 := keys["dt"]
	require.True(t, ok1)
	assert.Equal(t, "1983-04-20-13", dt)

	// tg partition key is deprecated
	_, ok2 := keys["tg"]
	require.False(t, ok2)
}

func TestParquetLocationPartition(t *testing.T) {
	pqLoc := newGenericParquetLocation()
	assert.Equal(t, "dt=1983-04-20-13", pqLoc.Partition())
}
