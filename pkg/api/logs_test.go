package api_test

import (
	"encoding/json"
	"log"
	"testing"

	"github.com/itchyny/gojq"
	"github.com/m-mizutani/minerva/pkg/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustToJSON(v interface{}) string {
	raw, err := json.Marshal(v)
	if err != nil {
		log.Fatal("Fail to marshal JSON:", err)
	}
	return string(raw)
}

func mustParseJQ(query string) *gojq.Query {
	q, err := gojq.Parse(query)
	if err != nil {
		log.Fatal("Fail to parse jq query:", err)
	}
	return q
}

type actionLog struct {
	Name   string `json:"name"`
	Target string `json:"target"`
}

func newLogStream() chan *api.LogQueue {
	logs := [][]string{
		{"test.user", "1580000000", mustToJSON(map[string]interface{}{
			"name":  "Ao",
			"color": "blue",
			"rank":  0,
		})},
		{"test.user", "1580000002", mustToJSON(map[string]interface{}{
			"name":  "Tou",
			"color": "orange",
			"rank":  1,
		})},
		{"test.user", "1580000004", mustToJSON(map[string]interface{}{
			"name": "Alice",
			"rank": 100,
		})},
		{"test.user", "1580000005", mustToJSON(map[string]interface{}{
			"name": "Barth",
			"rank": 1,
		})},
		{"test.user", "1580000009", mustToJSON(map[string]interface{}{
			"name": "Chris",
			"rank": 200,
		})},
		{"test.user", "1580000010", mustToJSON(map[string]interface{}{
			"name": "Dymos",
			"rank": 300,
		})},
		{"test.action", "1580000012", mustToJSON(actionLog{
			Name:   "attack",
			Target: "rock",
		})},
		{"test.action", "1580000012", mustToJSON(actionLog{
			Name:   "Ao",
			Target: "paper",
		})},
	}

	ch := make(chan *api.LogQueue, 1)
	go func() {
		defer close(ch)
		for seq, log := range logs {
			ch <- &api.LogQueue{
				Seq:    int64(seq),
				Record: log,
			}
		}
	}()
	return ch
}

func TestExtractLogsLimit(t *testing.T) {
	filter := api.LogFilter{
		Offset: 0,
		Limit:  3,
	}

	logSet, err := api.ExtractLogs(newLogStream(), filter)
	require.NoError(t, err)
	assert.Equal(t, 3, len(logSet.Logs))
	assert.Equal(t, int64(8), logSet.Total)
	assert.Equal(t, int64(8), logSet.SubTotal)
	assert.Contains(t, logSet.Tags, "test.user")
	assert.Equal(t, "Ao", logSet.Logs[0].Log.(map[string]interface{})["name"].(string))
	assert.Equal(t, "Tou", logSet.Logs[1].Log.(map[string]interface{})["name"].(string))
	assert.Equal(t, "Alice", logSet.Logs[2].Log.(map[string]interface{})["name"].(string))
}

func TestExtractLogsQuerySelect(t *testing.T) {
	filter := api.LogFilter{
		Offset: 0,
		Limit:  3,
		Query:  mustParseJQ(`select(.name == "Ao")`),
	}

	logSet, err := api.ExtractLogs(newLogStream(), filter)
	require.NoError(t, err)
	assert.Equal(t, 2, len(logSet.Logs))
	assert.Equal(t, int64(8), logSet.Total)
	assert.Equal(t, int64(2), logSet.SubTotal)

	assert.Contains(t, logSet.Tags, "test.user")
	assert.Contains(t, logSet.Tags, "test.action")
	assert.Equal(t, 2, len(logSet.Tags))

	assert.Equal(t, "Ao", logSet.Logs[0].Log.(map[string]interface{})["name"].(string))
	assert.Equal(t, "Ao", logSet.Logs[1].Log.(map[string]interface{})["name"].(string))
}

func TestExtractLogsQueryItem(t *testing.T) {
	filter := api.LogFilter{
		Offset: 0,
		Limit:  3,
		Query:  mustParseJQ(`.target`),
	}

	logSet, err := api.ExtractLogs(newLogStream(), filter)
	require.NoError(t, err)

	// Exclude no value logs filtered by jq .xxx query
	assert.Equal(t, 2, len(logSet.Logs))
	assert.Equal(t, int64(8), logSet.Total)
	assert.Equal(t, int64(2), logSet.SubTotal)

	assert.Contains(t, logSet.Tags, "test.user")
	assert.Contains(t, logSet.Tags, "test.action")
	assert.Equal(t, 2, len(logSet.Tags))

	// In order to keep key-value style, add blank key to store string value.
	assert.Equal(t, "rock", logSet.Logs[0].Log.(map[string]string)[""])
	assert.Equal(t, "paper", logSet.Logs[1].Log.(map[string]string)[""])
}

func TestExtractLogsQueryItemLimit(t *testing.T) {
	// Check if limit and offset work well with jq's query.
	logSet1, err := api.ExtractLogs(newLogStream(), api.LogFilter{
		Offset: 0,
		Limit:  1,
		Query:  mustParseJQ(`.target`),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, len(logSet1.Logs))
	assert.Equal(t, "rock", logSet1.Logs[0].Log.(map[string]string)[""])

	logSet2, err := api.ExtractLogs(newLogStream(), api.LogFilter{
		Offset: 1,
		Limit:  3,
		Query:  mustParseJQ(`.target`),
	})
	require.NoError(t, err)
	assert.Equal(t, 1, len(logSet2.Logs))
	assert.Equal(t, "paper", logSet2.Logs[0].Log.(map[string]string)[""])
}
