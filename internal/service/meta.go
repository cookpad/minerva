package service

import (
	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/pkg/models"
)

// MetaService is accessor of MetaRepository
type MetaService struct {
	repo              repository.MetaRepository
	cacheObjectID     map[string]int64
	cachePartitionKey map[string]bool
}

// NewMetaService is constructor of MetaService
func NewMetaService(repo repository.MetaRepository) *MetaService {
	return &MetaService{
		repo:              repo,
		cacheObjectID:     make(map[string]int64),
		cachePartitionKey: make(map[string]bool),
	}
}

// GetObjectID provides objectID that is unique ID for S3 object
func (x *MetaService) GetObjectID(s3Bucket, s3Key string) (int64, error) {
	s3path := s3Bucket + "/" + s3Key
	if id, ok := x.cacheObjectID[s3path]; ok {
		return id, nil
	}

	id, err := x.repo.GetObjecID(s3path)
	if err != nil {
		return 0, err
	}

	x.cacheObjectID[s3path] = id
	return id, nil
}

// PutObjects puts set of MetaRecordObject
func (x *MetaService) PutObjects(items []*repository.MetaRecordObject) error {
	return x.repo.PutRecordObjects(items)
}

//GetObjects retrieves set of MetaRecordObject and converts them to []*models.S3Object
func (x *MetaService) GetObjects(recordIDs []string, schema models.ParquetSchemaName) ([]*models.S3Object, error) {
	items, err := x.repo.GetRecordObjects(recordIDs, schema)
	if err != nil {
		return nil, err
	}

	var objects []*models.S3Object
	for _, item := range items {
		objects = append(objects, &item.S3Object)
	}
	return objects, nil
}

// HeadPartition checks an existance of partition and cache the result.
func (x *MetaService) HeadPartition(partitionKey string) (bool, error) {
	if exists, ok := x.cachePartitionKey[partitionKey]; ok && exists {
		return exists, nil
	}

	exists, err := x.repo.HeadPartition(partitionKey)
	if err != nil {
		return false, err
	}
	x.cachePartitionKey[partitionKey] = exists
	return exists, nil
}

// PutPartition register an existance of partition and cache the result.
func (x *MetaService) PutPartition(partitionKey string) error {
	if err := x.repo.PutPartition(partitionKey); err != nil {
		return err
	}
	x.cachePartitionKey[partitionKey] = true
	return nil
}
