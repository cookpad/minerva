package repository_test

import (
	"os"
	"testing"

	"github.com/google/uuid"
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectID(t *testing.T) {
	region := os.Getenv("MINERVA_TEST_REGION")
	table := os.Getenv("MINERVA_TEST_TABLE")

	if region == "" || table == "" {
		t.Skip("Both of MINERVA_TEST_REGION and MINERVA_TEST_TABLE are required")
	}

	meta := repository.NewMetaDynamoDB(region, table)
	id1, err := meta.GetObjecID("b1", "k1")
	require.NoError(t, err)
	id1a, err := meta.GetObjecID("b1", "k1")
	require.NoError(t, err)

	id2, err := meta.GetObjecID("b1", "k2")
	require.NoError(t, err)

	assert.NotEqual(t, id1, id2)
	assert.Equal(t, id1, id1a)
	assert.NotEqual(t, 0, id1)
}

func TestPartition(t *testing.T) {
	region := os.Getenv("MINERVA_TEST_REGION")
	table := os.Getenv("MINERVA_TEST_TABLE")

	if region == "" || table == "" {
		t.Skip("Both of MINERVA_TEST_REGION and MINERVA_TEST_TABLE are required")
	}

	pkey := uuid.New().String()
	meta := repository.NewMetaDynamoDB(region, table)
	has, err := meta.HeadPartition(pkey)
	require.NoError(t, err)
	assert.False(t, has)

	err = meta.PutPartition(pkey)
	require.NoError(t, err)

	has, err = meta.HeadPartition(pkey)
	require.NoError(t, err)
	assert.True(t, has)
}
