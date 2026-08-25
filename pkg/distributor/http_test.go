package distributor_test

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"katrelgulumj/loki/pkg/distributor"
	"katrelgulumj/loki/pkg/validation"
)

func TestPushHandler(t *testing.T) {
	now := time.Date(2023, 10, 1, 12, 0, 0, 0, time.UTC)

	limits := &validation.Limits{
		RejectOldSamples:       true,
		RejectOldSamplesMaxAge: 1 * time.Hour,
		CreationGracePeriod:    10 * time.Minute,
		MaxLineSize:            100,
	}

	tests := []struct {
		name           string
		payload        string
		expectedStatus int
		expectedBody   string
		checkMetric    func(t *testing.T, d *distributor.Distributor)
	}{
		{
			name: "valid push returns 204 No Content",
			payload: fmt.Sprintf(`{
				"streams": [{
					"stream": {"app": "test"},
					"values": [["%d", "valid log entry"]
				}]}`, now.Add(-5*time.Minute).UnixNano()),
			expectedStatus: http.StatusNoContent,
		},
		{
			name: "expired timestamp returns 400 Bad Request",
			payload: fmt.Sprintf(`{
				"streams": [{
					"stream": {"app": "test"},
					"values": [["%d", "old log entry"]
				}]}`, now.Add(-24*time.Hour).UnixNano()),
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "timestamp_too_old",
			checkMetric: func(t *testing.T, d *distributor.Distributor) {
				if count := d.Metrics().GetDiscardedSamples("timestamp_too_old"); count != 1 {
					t.Errorf("expected 1 discarded sample for timestamp_too_old, got %d", count)
				}
			},
		},
		{
			name: "timestamp too far in future returns 400 Bad Request",
			payload: fmt.Sprintf(`{
				"streams": [{
					"stream": {"app": "test"},
					"values": [["%d", "future log entry"]
				}]}`, now.Add(2*time.Hour).UnixNano()),
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "timestamp_too_far_in_future",
			checkMetric: func(t *testing.T, d *distributor.Distributor) {
				if count := d.Metrics().GetDiscardedSamples("timestamp_too_far_in_future"); count != 1 {
					t.Errorf("expected 1 discarded sample for timestamp_too_far_in_future, got %d", count)
				}
			},
		},
		{
			name: "line size exceeding limit returns 400 Bad Request",
			payload: fmt.Sprintf(`{
				"streams": [{
					"stream": {"app": "test"},
					"values": [["%d", "%s"]
				}]}`, now.UnixNano(), strings.Repeat("a", 200)),
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "max_line_size_exceeded",
			checkMetric: func(t *testing.T, d *distributor.Distributor) {
				if count := d.Metrics().GetDiscardedSamples("max_line_size_exceeded"); count != 1 {
					t.Errorf("expected 1 discarded sample for max_line_size_exceeded, got %d", count)
				}
			},
		},
		{
			name: "out of order timestamps return 400 Bad Request",
			payload: fmt.Sprintf(`{
				"streams": [{
					"stream": {"app": "test"},
					"values": [
						["%d", "first entry"],
						["%d", "second out of order entry"]
					]
				}]}`, now.UnixNano(), now.Add(-5*time.Minute).UnixNano()),
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "entry_out_of_order",
			checkMetric: func(t *testing.T, d *distributor.Distributor) {
				if count := d.Metrics().GetDiscardedSamples("entry_out_of_order"); count != 1 {
					t.Errorf("expected 1 discarded sample for entry_out_of_order, got %d", count)
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := distributor.New(limits)
			d.NowFunc = func() time.Time { return now }
			handler := distributor.PushHandler(d)

			req := httptest.NewRequest(http.MethodPost, "/loki/api/v1/push", bytes.NewBufferString(tc.payload))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()

			handler(w, req)

			if w.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d; body: %s", tc.expectedStatus, w.Code, w.Body.String())
			}

			if tc.expectedBody != "" && !strings.Contains(w.Body.String(), tc.expectedBody) {
				t.Errorf("expected response body to contain %q, got %q", tc.expectedBody, w.Body.String())
			}

			if tc.checkMetric != nil {
				tc.checkMetric(t, d)
			}
		})
	}
}
