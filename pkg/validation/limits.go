package validation

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrRateLimitExceeded = errors.New("rate limit exceeded")
)

type Limits struct {
	RejectOldSamples       bool
	RejectOldSamplesMaxAge time.Duration
	CreationGracePeriod    time.Duration
	MaxLineSize            int
	MaxLineSizeTruncate    bool
	EnforceMetricName      bool
}

type SampleValidationError struct {
	Reason  string
	Message string
	Stream  map[string]string
}

func (e *SampleValidationError) Error() string {
	if len(e.Stream) > 0 {
		return fmt.Sprintf("entry rejected: %s (stream: %v, reason: %s)", e.Message, e.Stream, e.Reason)
	}
	return fmt.Sprintf("entry rejected: %s (reason: %s)", e.Message, e.Reason)
}

type MultiValidationError struct {
	Errors []error
}

func (m *MultiValidationError) Error() string {
	if len(m.Errors) == 0 {
		return "validation error"
	}
	msg := fmt.Sprintf("push batch contained %d rejected entry(ies):", len(m.Errors))
	for _, err := range m.Errors {
		msg += fmt.Sprintf("\n - %s", err.Error())
	}
	return msg
}
