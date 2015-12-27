package main

import (
	"sync"
	"time"
)

// Profiles performance
type PerformanceProfiler struct {
	RecentResults        []*PerformanceProfilerMeasurement
	RecentResultsPointer int
	WindowSize           int
	mux                  sync.RWMutex
}

// Single measurement
type PerformanceProfilerMeasurement struct {
	HasError  bool
	StartTime time.Time
	EndTime   time.Time
	Took      int64 // in nanoseconds

	// Reference
	profiler *PerformanceProfiler
}

// Stats
type PerformanceProfilerStats struct {
	AvgSuccessMs float64
	AvgErrorMs   float64
}

// Start profiling session
func (this *PerformanceProfiler) Start() *PerformanceProfilerMeasurement {
	return newPerformanceProfilerMeasurement(this)
}

// Add measurement
func (this *PerformanceProfiler) _addMeasurement(m *PerformanceProfilerMeasurement) {
	this.mux.Lock()
	this.RecentResults[this.RecentResultsPointer] = m
	this.RecentResultsPointer++
	if this.RecentResultsPointer == this.WindowSize {
		this.RecentResultsPointer = 0
	}
	this.mux.Unlock()
	s := this.Stats()
	log.Infof("Performance profiler error %t, took %fms (avg success: %fms - avg error: %fms)", m.HasError, m.Milliseconds(), s.AvgSuccessMs, s.AvgErrorMs)
}

// Stats
func (this *PerformanceProfiler) Stats() *PerformanceProfilerStats {
	s := &PerformanceProfilerStats{}
	this.mux.RLock()
	var successTotal float64 = 0
	var successCount float64 = 0
	var errorTotal float64 = 0
	var errorCount float64 = 0
	for _, res := range this.RecentResults {
		if res == nil {
			continue
		}
		if res.HasError {
			// Error
			errorCount++
			errorTotal += res.Milliseconds()
		} else {
			// Success
			successCount++
			successTotal += res.Milliseconds()
		}
	}
	this.mux.RUnlock()
	s.AvgSuccessMs = successTotal / successCount
	return s
}

// To milliseconds
func (this *PerformanceProfilerMeasurement) Milliseconds() float64 {
	return float64(this.Took) / 1000000
}

// Stop session, succesful
func (this *PerformanceProfilerMeasurement) Success() {
	this._stop()
}

// Stop session, error
func (this *PerformanceProfilerMeasurement) Error() {
	this.HasError = true
	this._stop()
}

// Stop session
func (this *PerformanceProfilerMeasurement) _stop() {
	// Finalize values
	this.EndTime = time.Now()
	this.Took = this.EndTime.Sub(this.StartTime).Nanoseconds()

	// Write to profile
	this.profiler._addMeasurement(this)
}

// New profiler
func newPerformanceProfiler() *PerformanceProfiler {
	n := 64
	return &PerformanceProfiler{
		WindowSize:    n,
		RecentResults: make([]*PerformanceProfilerMeasurement, n),
	}
}

// New entity
func newPerformanceProfilerMeasurement(p *PerformanceProfiler) *PerformanceProfilerMeasurement {
	return &PerformanceProfilerMeasurement{
		profiler:  p,
		StartTime: time.Now(),
		HasError:  false,
	}
}
