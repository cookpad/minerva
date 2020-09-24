package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestExplonentialBackoffTimer(t *testing.T) {
	// Equation: math.Pow(2.0, float64(x.retryCount)) / 8

	timer := &retryTimer{}
	t0 := timer.calcWaitTime()
	assert.Greater(t, int64(time.Second), int64(t0))

	t1 := timer.calcWaitTime()
	assert.Greater(t, int64(time.Second), int64(t1))

	t2 := timer.calcWaitTime()
	assert.Greater(t, int64(time.Second), int64(t2))

	t3 := timer.calcWaitTime()
	assert.Equal(t, time.Second, t3)

	t4 := timer.calcWaitTime()
	assert.Greater(t, int64(t4), int64(time.Second))
}
