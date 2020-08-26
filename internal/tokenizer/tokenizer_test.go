package tokenizer_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenizer(t *testing.T) {
	s := "connect to 192.168.10.1:8000 from 192.168.10.2 by ssh"
	x := NewSimpleTokenizer()
	tokens := x.Split(s)
	assert.Equal(t, 15, len(tokens))
	assert.Equal(t, "192.168.10.1", tokens[4].Data)
}
