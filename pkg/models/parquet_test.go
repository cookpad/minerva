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

	pqLoc.MergeStat = models.ParquetMergeStatMerged
	assert.Equal(t, "myprefix/messages/dt=1983-04-20-13/merged/srcbucket/srckey.parquet", pqLoc.S3Key())
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

func TestParquetLocationParseS3Key(t *testing.T) {
	pqLoc, err := models.ParseS3Key("myprefix/messages/dt=1983-04-20-13/merged/srcbucket/blue/orange/magic.parquet", "myprefix/")
	require.NoError(t, err)

	assert.Equal(t, models.ParquetSchemaMessage, pqLoc.Schema)
	assert.Equal(t, 1983, pqLoc.Timestamp.Year())
	assert.Equal(t, time.Month(4), pqLoc.Timestamp.Month())
	assert.Equal(t, 20, pqLoc.Timestamp.Day())
	assert.Equal(t, models.ParquetMergeStatMerged, pqLoc.MergeStat)
	assert.Equal(t, 13, pqLoc.Timestamp.Hour())
	assert.Equal(t, "srcbucket", pqLoc.SrcBucket)
	assert.Equal(t, "blue/orange/magic.parquet", pqLoc.SrcKey)

	// Change schema
	pqLoc, err = models.ParseS3Key("myprefix/indices/dt=1983-04-20-13/merged/srcbucket/blue/orange/magic.parquet", "myprefix/")
	require.NoError(t, err)
	assert.Equal(t, models.ParquetSchemaIndex, pqLoc.Schema)
	assert.Equal(t, 1983, pqLoc.Timestamp.Year())
	assert.Equal(t, time.Month(4), pqLoc.Timestamp.Month())
	assert.Equal(t, 20, pqLoc.Timestamp.Day())
	assert.Equal(t, models.ParquetMergeStatMerged, pqLoc.MergeStat)
	assert.Equal(t, 13, pqLoc.Timestamp.Hour())
	assert.Equal(t, "srcbucket", pqLoc.SrcBucket)
	assert.Equal(t, "blue/orange/magic.parquet", pqLoc.SrcKey)

	// Change merge stat
	pqLoc, err = models.ParseS3Key("myprefix/indices/dt=1983-04-20-13/unmerged/srcbucket/blue/orange/magic.parquet", "myprefix/")
	require.NoError(t, err)
	assert.Equal(t, models.ParquetSchemaIndex, pqLoc.Schema)
	assert.Equal(t, 1983, pqLoc.Timestamp.Year())
	assert.Equal(t, time.Month(4), pqLoc.Timestamp.Month())
	assert.Equal(t, 20, pqLoc.Timestamp.Day())
	assert.Equal(t, models.ParquetMergeStatUnmerged, pqLoc.MergeStat)
	assert.Equal(t, 13, pqLoc.Timestamp.Hour())
	assert.Equal(t, "srcbucket", pqLoc.SrcBucket)
	assert.Equal(t, "blue/orange/magic.parquet", pqLoc.SrcKey)

	// No hour, src bucket and src key. Additionally no prefix
	pqLoc, err = models.ParseS3Key("indices/dt=1983-04-20-13/unmerged/", "")
	require.NoError(t, err)
	assert.Equal(t, models.ParquetSchemaIndex, pqLoc.Schema)
	assert.Equal(t, 1983, pqLoc.Timestamp.Year())
	assert.Equal(t, time.Month(4), pqLoc.Timestamp.Month())
	assert.Equal(t, 20, pqLoc.Timestamp.Day())
	assert.Equal(t, models.ParquetMergeStatUnmerged, pqLoc.MergeStat)
	assert.Equal(t, 13, pqLoc.Timestamp.Hour())
	assert.Equal(t, "", pqLoc.SrcBucket)
	assert.Equal(t, "", pqLoc.SrcKey)
}

func TestParquetLocationParseS3KeyError(t *testing.T) {
	// Prefix mismatch
	ptr, err := models.ParseS3Key("yourprefix/messages/dt=1983-04-20-13/merged/srcbucket/blue/orange/magic.parquet", "myprefix/")
	assert.Nil(t, ptr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Prefix is not matched")

	// Invalid schema
	ptr, err = models.ParseS3Key("myprefix/red/dt=1983-04-20-13/merged/srcbucket/blue/orange/magic.parquet", "myprefix/")
	assert.Nil(t, ptr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid schema name")

	// Invalid dt key
	ptr, err = models.ParseS3Key("myprefix/messages/dtx=1983-04-20-13/merged/srcbucket/blue/orange/magic.parquet", "myprefix/")
	assert.Nil(t, ptr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid partition key (dt)")

	// Invalid tg key
	ptr, err = models.ParseS3Key("myprefix/messages/tag=mylog/dt=1983-04-20-13/merged/srcbucket/blue/orange/magic.parquet", "myprefix/")
	assert.Nil(t, ptr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid partition key (dt)")

	// Invalid merge stat
	ptr, err = models.ParseS3Key("myprefix/messages/dt=1983-04-20-13/unknown/srcbucket/blue/orange/magic.parquet", "myprefix/")
	assert.Nil(t, ptr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid merge status")

	// Invalid dt key format
	ptr, err = models.ParseS3Key("myprefix/messages/dt=1983-04-20-13T00/merged/srcbucket/blue/orange/magic.parquet", "myprefix/")
	assert.Nil(t, ptr)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "Fail to parse dt key")
}
