package mock

import (
	"math/rand"

	"github.com/m-mizutani/minerva/internal/repository"
	"github.com/m-mizutani/minerva/pkg/models"
)

type MetaRepository struct {
	idMap        map[string]int64
	pathMap      map[string]map[string]*repository.MetaRecordObject
	partitionMap map[string]bool
}

func NewMetaRepository() repository.MetaRepository {
	return &MetaRepository{
		idMap:        make(map[string]int64),
		pathMap:      make(map[string]map[string]*repository.MetaRecordObject),
		partitionMap: make(map[string]bool),
	}
}

func (x *MetaRepository) GetObjecID(s3path string) (int64, error) {
	if id, ok := x.idMap[s3path]; ok {
		return id, nil
	}

	id := rand.Int63()
	x.idMap[s3path] = id
	return id, nil
}

func (x *MetaRepository) PutRecordObjects(objects []*repository.MetaRecordObject) error {
	for _, path := range objects {
		schemaMap, ok := x.pathMap[path.RecordID]
		if !ok {
			schemaMap = make(map[string]*repository.MetaRecordObject)
			x.pathMap[path.RecordID] = schemaMap
		}
		schemaMap[string(path.Schema)] = path
	}

	return nil
}

func (x *MetaRepository) GetRecordObjects(recordIDs []string, schema models.ParquetSchemaName) ([]*repository.MetaRecordObject, error) {
	var results []*repository.MetaRecordObject
	for _, id := range recordIDs {
		if schemaMap, ok := x.pathMap[id]; ok {
			if path, ok := schemaMap[string(schema)]; ok {
				results = append(results, path)
			}
		}
	}
	return results, nil
}

func (x *MetaRepository) HeadPartition(partitionKey string) (bool, error) {
	if exists, ok := x.partitionMap[partitionKey]; ok {
		return exists, nil
	}
	return false, nil
}

func (x *MetaRepository) PutPartition(partitionKey string) error {
	x.partitionMap[partitionKey] = true
	return nil
}
