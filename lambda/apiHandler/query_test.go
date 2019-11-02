package main_test

import (
	"testing"
	"time"

	main "github.com/m-mizutani/minerva/lambda/apiHandler"
	"github.com/stretchr/testify/assert"
)

func TestArgsToSQL(t *testing.T) {
	q := main.NewRequest(
		[]string{"mizutani@cookpad.com"},
		time.Date(2019, 10, 24, 11, 14, 15, 0, time.UTC),
		time.Date(2019, 10, 24, 15, 14, 15, 0, time.UTC),
	)

	sql, err := main.ArgsToSQL(q, "indices", "messages")
	assert.NoError(t, err)
	assert.Contains(t, *sql, "SELECT indices.tag")
	assert.Contains(t, *sql, ", messages.timestamp")
	assert.Contains(t, *sql, ", messages.message")
	assert.Contains(t, *sql, "term = 'mizutani'")
	assert.Contains(t, *sql, "term = 'cookpad'")
	assert.Contains(t, *sql, "term = 'com'")
}
