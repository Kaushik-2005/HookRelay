// Command bench runs a reproducible concurrent HTTP workload and reports
// throughput plus p50/p95/p99 latency. Point it at an API endpoint or a
// controlled webhook test endpoint.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	latency time.Duration
	status  int
	err     error
}

func main() {
	url := flag.String("url", "http://localhost:8080/health", "URL to benchmark")
	requests := flag.Int("requests", 1000, "total requests")
	concurrency := flag.Int("concurrency", 25, "concurrent workers")
	method := flag.String("method", http.MethodGet, "HTTP method")
	timeout := flag.Duration("timeout", 15*time.Second, "per-request timeout")
	flag.Parse()
	if *requests < 1 || *concurrency < 1 {
		panic("requests and concurrency must be positive")
	}

	client := &http.Client{Timeout: *timeout}
	jobs := make(chan struct{})
	results := make(chan result, *requests)
	var completed atomic.Int64
	started := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				requestStarted := time.Now()
				request, err := http.NewRequestWithContext(context.Background(), *method, *url, nil)
				if err != nil {
					results <- result{latency: time.Since(requestStarted), err: err}
					continue
				}
				response, err := client.Do(request)
				if err != nil {
					results <- result{latency: time.Since(requestStarted), err: err}
					continue
				}
				_, _ = io.Copy(io.Discard, response.Body)
				_ = response.Body.Close()
				results <- result{latency: time.Since(requestStarted), status: response.StatusCode}
				completed.Add(1)
			}
		}()
	}
	go func() {
		for i := 0; i < *requests; i++ {
			jobs <- struct{}{}
		}
		close(jobs)
		wg.Wait()
		close(results)
	}()

	latencies := make([]time.Duration, 0, *requests)
	statuses := map[int]int{}
	errors := 0
	for item := range results {
		latencies = append(latencies, item.latency)
		if item.err != nil {
			errors++
		} else {
			statuses[item.status]++
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	elapsed := time.Since(started)
	fmt.Printf("url=%s requests=%d concurrency=%d elapsed=%s throughput=%.2f req/s completed=%d errors=%d\n", *url, *requests, *concurrency, elapsed.Round(time.Millisecond), float64(*requests)/elapsed.Seconds(), completed.Load(), errors)
	fmt.Printf("latency p50=%s p95=%s p99=%s\n", percentile(latencies, .50), percentile(latencies, .95), percentile(latencies, .99))
	fmt.Printf("statuses=%v\n", statuses)
}

func percentile(values []time.Duration, percentile float64) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := int(float64(len(values)-1) * percentile)
	return values[index].Round(time.Microsecond)
}
