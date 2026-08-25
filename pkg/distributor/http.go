package distributor

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"katrelgulumj/loki/pkg/validation"
)

func PushHandler(d *Distributor) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "failed to read request body: "+err.Error(), http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		if len(body) == 0 {
			http.Error(w, "empty request body", http.StatusBadRequest)
			return
		}

		var req PushRequest
		if err := json.Unmarshal(body, &req); err != nil {
			http.Error(w, "failed to parse push payload: "+err.Error(), http.StatusBadRequest)
			return
		}

		err = d.Push(r.Context(), &req)
		if err != nil {
			if errors.Is(err, validation.ErrRateLimitExceeded) {
				http.Error(w, err.Error(), http.StatusTooManyRequests)
				return
			}

			var validationErr *validation.SampleValidationError
			var multiErr *validation.MultiValidationError
			if errors.As(err, &validationErr) || errors.As(err, &multiErr) {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}
