package distributor

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"katrelgulumj/loki/pkg/validation"
)

type Entry struct {
	Timestamp time.Time `json:"timestamp"`
	Line      string    `json:"line"`
}

type Stream struct {
	Labels  map[string]string `json:"stream"`
	Entries []Entry           `json:"entries"`
	Values  [][]string        `json:"values,omitempty"`
}

type PushRequest struct {
	Streams []Stream `json:"streams"`
}

type Metrics struct {
	mu                   sync.RWMutex
	DiscardedSamplesTotal map[string]int64
	DiscardedBytesTotal   map[string]int64
}

func NewMetrics() *Metrics {
	return &Metrics{
		DiscardedSamplesTotal: make(map[string]int64),
		DiscardedBytesTotal:   make(map[string]int64),
	}
}

func (m *Metrics) RecordDiscard(reason string, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.DiscardedSamplesTotal[reason]++
	m.DiscardedBytesTotal[reason] += bytes
}

func (m *Metrics) GetDiscardedSamples(reason string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.DiscardedSamplesTotal[reason]
}

func (m *Metrics) GetDiscardedBytes(reason string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.DiscardedBytesTotal[reason]
}

type Distributor struct {
	limits  *validation.Limits
	metrics *Metrics
	NowFunc func() time.Time
}

func New(limits *validation.Limits) *Distributor {
	if limits == nil {
		limits = &validation.Limits{}
	}
	return &Distributor{
		limits:  limits,
		metrics: NewMetrics(),
		NowFunc: time.Now,
	}
}

func (d *Distributor) Metrics() *Metrics {
	return d.metrics
}

func (d *Distributor) Push(ctx context.Context, req *PushRequest) error {
	if req == nil || len(req.Streams) == 0 {
		return nil
	}

	now := d.NowFunc()
	var validationErrors []error

	for _, stream := range req.Streams {
		// Parse values if entries not explicitly filled
		entries := stream.Entries
		if len(entries) == 0 && len(stream.Values) > 0 {
			for _, val := range stream.Values {
				if len(val) >= 2 {
					tsNano, err := strconv.ParseInt(val[0], 10, 64)
					if err != nil {
						validationErrors = append(validationErrors, &validation.SampleValidationError{
							Reason:  "invalid_timestamp",
							Message: fmt.Sprintf("unable to parse timestamp %s", val[0]),
							Stream:  stream.Labels,
						})
						d.metrics.RecordDiscard("invalid_timestamp", int64(len(val[1])))
						continue
					}
					entries = append(entries, Entry{
						Timestamp: time.Unix(0, tsNano),
						Line:      val[1],
					})
				}
			}
		}

		var lastTimestamp time.Time
		for _, entry := range entries {
			lineBytes := int64(len(entry.Line))

			// 1. Line size limit validation
			if d.limits.MaxLineSize > 0 && len(entry.Line) > d.limits.MaxLineSize {
				d.metrics.RecordDiscard("max_line_size_exceeded", lineBytes)
				validationErrors = append(validationErrors, &validation.SampleValidationError{
					Reason:  "max_line_size_exceeded",
					Message: fmt.Sprintf("entry line size %d exceeds max line size %d", len(entry.Line), d.limits.MaxLineSize),
					Stream:  stream.Labels,
				})
				continue
			}

			// 2. Reject old samples validation
			if d.limits.RejectOldSamples && d.limits.RejectOldSamplesMaxAge > 0 {
				cutoff := now.Add(-d.limits.RejectOldSamplesMaxAge)
				if entry.Timestamp.Before(cutoff) {
					d.metrics.RecordDiscard("timestamp_too_old", lineBytes)
					validationErrors = append(validationErrors, &validation.SampleValidationError{
						Reason:  "timestamp_too_old",
						Message: fmt.Sprintf("entry too old: timestamp %s is older than max age limit %s", entry.Timestamp.UTC(), d.limits.RejectOldSamplesMaxAge),
						Stream:  stream.Labels,
					})
					continue
				}
			}

			// 3. Reject future samples validation
			if d.limits.CreationGracePeriod > 0 {
				futureCutoff := now.Add(d.limits.CreationGracePeriod)
				if entry.Timestamp.After(futureCutoff) {
					d.metrics.RecordDiscard("timestamp_too_far_in_future", lineBytes)
					validationErrors = append(validationErrors, &validation.SampleValidationError{
						Reason:  "timestamp_too_far_in_future",
						Message: fmt.Sprintf("entry too far in future: timestamp %s is beyond creation grace period %s", entry.Timestamp.UTC(), d.limits.CreationGracePeriod),
						Stream:  stream.Labels,
					})
					continue
				}
			}

			// 4. Out of order validation within stream
			if !lastTimestamp.IsZero() && entry.Timestamp.Before(lastTimestamp) {
				d.metrics.RecordDiscard("entry_out_of_order", lineBytes)
				validationErrors = append(validationErrors, &validation.SampleValidationError{
					Reason:  "entry_out_of_order",
					Message: fmt.Sprintf("entry out of order: timestamp %s is before previous timestamp %s", entry.Timestamp.UTC(), lastTimestamp.UTC()),
					Stream:  stream.Labels,
				})
				continue
			}

			lastTimestamp = entry.Timestamp
		}
	}

	if len(validationErrors) > 0 {
		return &validation.MultiValidationError{Errors: validationErrors}
	}

	return nil
}
