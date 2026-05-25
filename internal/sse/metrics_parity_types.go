// Package sse — JSON-serialisable fixture types shared by the build-tagged
// capture harness (golden_metrics_capture_test.go) and the default-build
// parity test (metrics_parity_test.go). Lives outside _test.go because the
// two consumers compile under different build tags; a normal source file
// (no tag) compiles in both. Types are unexported so external callers
// cannot depend on them.
package sse

// fixture is the on-disk JSON shape for scripts/golden/metrics_v13.json.
type fixture struct {
	ScenarioName string            `json:"scenario_name"`
	Description  string            `json:"description"`
	Deltas       []counterDelta    `json:"deltas"`
	Histograms   []histogramAssert `json:"histograms"`
}

// counterDelta locks one (name, label-set, delta) tuple. The delta is the
// absolute counter value captured from a per-test prometheus.NewRegistry
// (scenario-scoped, not process-scoped).
type counterDelta struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	Delta  float64           `json:"delta"`
}

// histogramAssert locks one (name, MinCount) tuple. MinCount is the floor
// on *Histogram.SampleCount; not asserted on Sum or bucket distribution
// (timing-sensitive).
type histogramAssert struct {
	Name     string `json:"name"`
	MinCount uint64 `json:"min_count"`
}
