package main

import "time"

var (
	ArgsToSQL  = argsToSQL
	NewRequest = newRequest
)

func newRequest(queries []string, start, end time.Time) request {
	return request{
		Queries:       queries,
		StartDateTime: start,
		EndDateTime:   end,
	}
}
