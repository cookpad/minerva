package api

var (
	BuildSQL   = buildSQL
	NewRequest = newRequest
)

type LogFilter logFilter
type LogDataSet logDataSet
type LogQueue logQueue
type SearchID searchID

func ExtractLogs(ch chan *LogQueue, filter LogFilter) (*LogDataSet, error) {
	pipe := make(chan *logQueue)
	go func() {
		defer close(pipe)
		for q := range ch {
			pipe <- (*logQueue)(q)
		}
	}()
	v, err := extractLogs(pipe, logFilter(filter))
	return (*LogDataSet)(v), err
}

func newRequest(terms []string, start, end string) ExecSearchRequest {
	var querySet []query
	for _, t := range terms {
		querySet = append(querySet, query{Term: t})
	}

	return ExecSearchRequest{
		Query:         querySet,
		StartDateTime: start,
		EndDateTime:   end,
	}
}
