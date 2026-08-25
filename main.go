package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"katrelgulumj/loki/pkg/distributor"
	"katrelgulumj/loki/pkg/validation"
)

func main() {
	limits := &validation.Limits{
		RejectOldSamples:       true,
		RejectOldSamplesMaxAge: 1 * time.Hour,
		CreationGracePeriod:    10 * time.Minute,
		MaxLineSize:            256 * 1024,
	}

	d := distributor.New(limits)
	http.HandleFunc("/loki/api/v1/push", distributor.PushHandler(d))

	port := ":3100"
	fmt.Printf("Loki distributor server listening on %s\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
