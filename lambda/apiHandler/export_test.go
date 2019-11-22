package main

var (
	BuildSQL   = buildSQL
	NewRequest = newRequest
)

func newRequest(terms []string, start, end string) request {
	var querySet []query
	for _, t := range terms {
		querySet = append(querySet, query{Term: t})
	}

	return request{
		Query:         querySet,
		StartDateTime: start,
		EndDateTime:   end,
	}
}
