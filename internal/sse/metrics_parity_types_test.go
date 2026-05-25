package sse

type fixture struct {
	ScenarioName string            `json:"scenario_name"`
	Description  string            `json:"description"`
	Deltas       []counterDelta    `json:"deltas"`
	Histograms   []histogramAssert `json:"histograms"`
}

type counterDelta struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	Delta  float64           `json:"delta"`
}

type histogramAssert struct {
	Name     string `json:"name"`
	MinCount uint64 `json:"min_count"`
}
