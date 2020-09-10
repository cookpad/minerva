package main

import "time"

type retryTimer struct {
	retryCount int
}

func (x *retryTimer) sleep() {
	time.Sleep(time.Second * 5)
}

func (x *retryTimer) clear() {
	x.retryCount = 0
}
