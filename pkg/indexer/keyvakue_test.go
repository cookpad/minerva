package indexer_test

import (
	"testing"

	"github.com/m-mizutani/minerva/pkg/indexer"
	"github.com/stretchr/testify/assert"
)

func TestKeyValuePair(t *testing.T) {
	type TestA struct {
		Num       int
		Str       string
		Array     []string
		StrMap    map[string]int
		JSONField string `json:"json_field"`
	}

	v := TestA{
		Num: 5,
		Str: "five",
		Array: []string{
			"not",
			"sane",
		},
		StrMap: map[string]int{
			"magic": 5,
		},
		JSONField: "xxx",
	}

	kvList := indexer.ToKeyValuePairs(v, "", false)
	assert.Equal(t, 5, indexer.LookupValue(kvList, "Num"))

	assert.NotNil(t, indexer.LookupValue(kvList, "Str"))
	assert.Equal(t, "five", indexer.LookupValue(kvList, "Str"))

	assert.Nil(t, indexer.LookupValue(kvList, "Array"))
	assert.Equal(t, "not", indexer.LookupValue(kvList, "Array.0"))
	assert.Equal(t, "sane", indexer.LookupValue(kvList, "Array.1"))
	assert.Nil(t, indexer.LookupValue(kvList, "Array.2"))

	assert.Nil(t, indexer.LookupValue(kvList, "StrMap"))
	assert.Equal(t, 5, indexer.LookupValue(kvList, "StrMap.magic"))

	assert.Nil(t, indexer.LookupValue(kvList, "JSONField"))
	assert.Equal(t, "xxx", indexer.LookupValue(kvList, "json_field"))
}
