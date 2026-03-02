package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"bidsrv/internal/generator"
)

var (
	bidCount        = flag.Int("bid-count", 10000, "Number of bid requests to generate")
	impressionCount = flag.Int("impression-count", 10000, "Number of impressions to generate")
	serverURL       = flag.String("server-url", "http://localhost:8080", "Bidding server URL")
	concurrency     = flag.Int("concurrency", 10, "Number of concurrent workers")
	cornerCaseRatio = flag.Float64("corner-case-ratio", 0.1, "Ratio of corner cases (0.0-1.0)")
	batchSize       = flag.Int("batch-size", 100, "Batch size for progress reporting")
)

func main() {
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	log.Printf("Starting event generator...")
	log.Printf("Target bids: %d, impressions: %d", *bidCount, *impressionCount)
	log.Printf("Server URL: %s", *serverURL)
	log.Printf("Concurrency: %d", *concurrency)
	log.Printf("Corner case ratio: %.2f%%", *cornerCaseRatio*100)

	// Create generator client
	client := generator.NewClient(*serverURL)

	// Stats counters
	var successBids, failedBids, successImpressions, failedImpressions int64
	var wg sync.WaitGroup

	// Worker pool for bids
	bidChan := make(chan int, *concurrency)

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Received shutdown signal, stopping...")
		cancel()
	}()

	// Start bid workers
	startTime := time.Now()

	log.Println("Starting bid generation workers...")
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for idx := range bidChan {
				select {
				case <-ctx.Done():
					return
				default:
				}

				// Determine if this should be a corner case
				isCornerCase := rand.Float64() < *cornerCaseRatio

				// Generate bid request
				bidReq := generator.GenerateBidRequest(isCornerCase)

				resp, err := client.CreateBid(ctx, bidReq)
				if err != nil {
					atomic.AddInt64(&failedBids, 1)
					log.Printf("[Worker %d] Failed to create bid: %v", workerID, err)
					continue
				}

				atomic.AddInt64(&successBids, 1)

				// Try to create impression for this bid
				// Some bids won't have impressions (for realistic view rate)
				if idx < *impressionCount {
					isImpCornerCase := rand.Float64() < *cornerCaseRatio
					impReq := generator.GenerateImpressionRequest(resp.BidID, isImpCornerCase)

					_, err := client.CreateImpression(ctx, impReq)
					if err != nil {
						atomic.AddInt64(&failedImpressions, 1)
						log.Printf("[Worker %d] Failed to create impression for bid %s: %v", workerID, resp.BidID, err)
						continue
					}
					atomic.AddInt64(&successImpressions, 1)
				}

				// Progress reporting
				totalDone := atomic.LoadInt64(&successBids) + atomic.LoadInt64(&failedBids)
				if totalDone%int64(*batchSize) == 0 {
					log.Printf("Progress: %d/%d bids, %d/%d impressions",
						totalDone, *bidCount,
						atomic.LoadInt64(&successImpressions)+atomic.LoadInt64(&failedImpressions),
						*impressionCount)
				}
			}
		}(i)
	}

	// Send jobs to workers
	for i := 0; i < *bidCount; i++ {
		select {
		case <-ctx.Done():
			break
		case bidChan <- i:
		}
	}
	close(bidChan)

	// Wait for all workers to complete
	wg.Wait()

	elapsed := time.Since(startTime)

	// Print final stats
	log.Println("========================================")
	log.Println("Generation Complete!")
	log.Println("========================================")
	log.Printf("Total time: %v", elapsed)
	log.Printf("Bids - Success: %d, Failed: %d, Total: %d", successBids, failedBids, successBids+failedBids)
	log.Printf("Impressions - Success: %d, Failed: %d, Total: %d", successImpressions, failedImpressions, successImpressions+failedImpressions)

	if successBids > 0 {
		log.Printf("Bid rate: %.2f req/sec", float64(successBids)/elapsed.Seconds())
	}
	if successImpressions > 0 {
		log.Printf("Impression rate: %.2f req/sec", float64(successImpressions)/elapsed.Seconds())
	}
	log.Println("========================================")

	// Check if we met the requirements
	if successBids >= 10000 && successImpressions >= 10000 {
		log.Println("SUCCESS: Generated >= 10k bids and >= 10k impressions")
	} else {
		log.Printf("WARNING: Did not meet target (bids: %d/10000, impressions: %d/10000)", successBids, successImpressions)
	}
}
