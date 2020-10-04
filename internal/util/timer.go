package util

import (
	"fmt"
	"math"
	"time"
)

// RetryTimer is interface retry
type RetryTimer interface {
	Run(RetryTimerCallback) error
}

// RetryTimerCallback is callback function type for RetryTimer
type RetryTimerCallback func(seq int) (exit bool, err error)

// RetryTimerFactory is
type RetryTimerFactory func(limit int) RetryTimer

// ErrRetryLimitExceeded indicates error message for exceeding limit of RetryTimer
var ErrRetryLimitExceeded = fmt.Errorf("Limit of RetryTimer exceeded")

type expRetryTimer struct {
	limit      int
	retryCount int
}

// NewExpRetryTimer is constructor of expRetryTimer (Exponential backoff timer)
func NewExpRetryTimer(limit int) RetryTimer {
	return &expRetryTimer{limit: limit}
}

func (x *expRetryTimer) Run(callback RetryTimerCallback) error {
	for i := 0; i < x.limit; i++ {
		exit, err := callback(i)
		if err != nil {
			return err
		}
		if exit {
			return nil
		}

		waitTime := x.calcWaitTime()
		time.Sleep(waitTime)
	}
	return fmt.Errorf("")
}

func (x *expRetryTimer) calcWaitTime() time.Duration {
	wait := math.Pow(2.0, float64(x.retryCount))/64 + 0.5
	if wait > 2 {
		wait = 2
	}
	mSec := time.Millisecond * time.Duration(wait*1000)
	x.retryCount++
	return mSec
}
