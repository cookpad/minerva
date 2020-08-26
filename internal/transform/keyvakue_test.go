package transform

import (
	"testing"

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

	kvList := toKeyValuePairs(v, "", false)
	assert.Equal(t, 5, lookupValue(kvList, "Num"))

	assert.NotNil(t, lookupValue(kvList, "Str"))
	assert.Equal(t, "five", lookupValue(kvList, "Str"))

	assert.Nil(t, lookupValue(kvList, "Array"))
	assert.Equal(t, "not", lookupValue(kvList, "Array.0"))
	assert.Equal(t, "sane", lookupValue(kvList, "Array.1"))
	assert.Nil(t, lookupValue(kvList, "Array.2"))

	assert.Nil(t, lookupValue(kvList, "StrMap"))
	assert.Equal(t, 5, lookupValue(kvList, "StrMap.magic"))

	assert.Nil(t, lookupValue(kvList, "JSONField"))
	assert.Equal(t, "xxx", lookupValue(kvList, "json_field"))
}

func lookupValue(kvList []keyValuePair, key string) interface{} {
	for _, kv := range kvList {
		if kv.Key == key {
			return kv.Value
		}
	}
	return nil
}
