package internal

import "time"

type profileRecord struct {
	current *time.Time
	total   time.Duration
	max     time.Duration
	count   int64
}

type profileResult struct {
	Total float64 `json:"total"`
	Max   float64 `json:"max"`
	Count int64   `json:"count"`
}

type Profile struct {
	Records map[string]*profileRecord `json:"records"`
}

func NewProfile() *Profile {
	return &Profile{
		Records: map[string]*profileRecord{},
	}
}

func (x *Profile) Start(target string) {
	p, ok := x.Records[target]
	if !ok {
		p = &profileRecord{}
		x.Records[target] = p
	}

	if p.current != nil {
		Logger.WithField("target", target).Fatal("target started twice for profile")
	}

	now := time.Now()
	p.current = &now
	p.count++
}

func (x *Profile) Stop(target string) {
	now := time.Now()

	p, ok := x.Records[target]
	if !ok {
		Logger.WithField("target", target).Fatal("Not started for profile")
	}

	sub := now.Sub(*p.current)
	p.total += sub
	if p.max < sub {
		p.max = sub
	}

	p.current = nil
}

func (x *Profile) Pack() map[string]profileResult {

	v := map[string]profileResult{}
	for k, r := range x.Records {
		v[k] = profileResult{
			Total: r.total.Seconds(),
			Max:   r.max.Seconds(),
			Count: r.count,
		}
	}
	return v
}
