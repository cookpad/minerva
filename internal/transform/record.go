package transform

import (
	"fmt"

	"github.com/m-mizutani/minerva/internal/tokenizer"
	"github.com/m-mizutani/minerva/pkg/models"
)

type LogToRecord func(q *models.LogQueue, objID int64) ([]interface{}, error)

type indexTerm struct {
	field string
	term  string
}

var globalTokenizer = tokenizer.NewSimpleTokenizer()

// LogToIndexRecord transforms from LogQueue to IndexRecord(s). Wrapper of logToIndexRecord.
func LogToIndexRecord(q *models.LogQueue, objID int64) ([]interface{}, error) {
	var out []interface{}
	terms := map[indexTerm]bool{}
	kvList := toKeyValuePairs(q.Value, "", false)

	for _, kv := range kvList {
		tokens := globalTokenizer.Split(fmt.Sprintf("%v", kv.Value))

		for _, token := range tokens {
			if token.IsDelim || token.IsSpace() {
				continue
			}

			t := indexTerm{field: kv.Key, term: token.Data}
			terms[t] = true
		}
	}

	for it := range terms {
		rec := models.IndexRecord{
			Tag:       q.Tag,
			Timestamp: q.Timestamp.Unix(),
			Field:     it.field,
			Term:      it.term,
			ObjectID:  objID,
			Seq:       int32(q.Seq),
		}
		out = append(out, rec)
	}

	return out, nil
}

// LogToMessageRecord transforms from LogQueue to MessageRecord(s)
func LogToMessageRecord(q *models.LogQueue, objID int64) ([]interface{}, error) {
	rec := models.MessageRecord{
		Timestamp: q.Timestamp.Unix(),
		Message:   q.Message,
		ObjectID:  objID,
		Seq:       q.Seq,
	}

	return []interface{}{rec}, nil
}
