package main

import (
	"bidsrv/internal/storage/clickhouse"
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-redis/redis/v8"

	"bidsrv/internal/api"
	"bidsrv/internal/campaign"
	"bidsrv/internal/consumer"
	"bidsrv/internal/event"
	"bidsrv/internal/idempotency"
	"bidsrv/internal/storage/infra_redis"
	"bidsrv/internal/storage/metrics"
)

func main() {
	redisAddr := getEnv("REDIS_ADDR", "localhost:6379")
	kafkaBrokers := getEnv("KAFKA_BROKERS", "localhost:9092")
	chAddr := getEnv("CLICKHOUSE_ADDR", "localhost:9000")
	port := getEnv("PORT", "8080")
	baseURL := getEnv("BASE_URL", "http://localhost:8080")

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisAddr,
	})

	// 1. Init Redis Idempotency Store
	store, err := idempotency.NewStore(redisClient)
	if err != nil {
		log.Fatalf("failed to init infra_redis: %v", err)
	}

	// 2. Init Redis Bid Cache
	bidCache, err := infra_redis.NewBidCache(redisClient)
	if err != nil {
		log.Fatalf("failed to init bid cache: %v", err)
	}

	// 3. Init Kafka Producer
	brokers := strings.Split(kafkaBrokers, ",")
	producer, err := event.NewProducer(brokers)
	if err != nil {
		log.Fatalf("failed to init redpanda producer: %v", err)
	}
	defer producer.Close()

	// 4. Init Campaign Manager
	campaignMgr := campaign.NewManager()

	// 5. Init Handlers
	handler := api.NewHandler(campaignMgr, store, bidCache, producer, baseURL)

	// 6. Init Consumer and start it
	chClient, err := clickhouse.NewClient(chAddr)
	if err != nil {
		log.Fatalf("failed to init clickhouse client: %v", err)
	}

	cons, err := consumer.NewConsumer(brokers, chClient, redisClient)
	if err != nil {
		log.Fatalf("warning: failed to init consumer: %v", err)
	}
	go func() {
		if err := cons.Start(context.Background()); err != nil {
			log.Fatalf("consumer error: %v", err)
		}
	}()

	// 7. Init Metrics Query Service for Dashboard
	queryService, err := metrics.NewQueryService(chClient, redisClient)
	if err != nil {
		log.Fatalf("failed to init metrics query service: %v", err)
	}

	// 8. Start background metrics aggregation thread
	stopChan := make(chan struct{})
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		log.Println("Background metrics aggregation thread started")

		for {
			select {
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
				minutes, err := chClient.GetMinutesToAggregate(ctx)
				if err != nil {
					log.Printf("Error getting minutes to aggregate: %v", err)
					cancel()
					continue
				}

				if len(minutes) > 0 {
					log.Printf("Aggregating %d minutes: %v", len(minutes), minutes)
					for _, minute := range minutes {
						if err := chClient.AggregateMetrics(ctx, minute); err != nil {
							log.Printf("Error aggregating metrics for minute %s: %v", minute, err)
						}
					}
				}

				cancel()
			case <-stopChan:
				log.Println("Background metrics aggregation thread stopping")
				return
			}
		}
	}()

	// 9. Init Dashboard Handler
	dashboardHandler := api.NewDashboardHandler(queryService)

	// 10. Setup Router
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	//r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", handler.HandleHealthz)
	r.Post("/v1/bid", handler.HandleBid)
	r.Post("/v1/billing", handler.HandleBilling)

	// Dashboard routes
	r.Get("/dashboard", dashboardHandler.HandleDashboard)
	r.Get("/dashboard/timeseries", dashboardHandler.HandleDashboardTimeSeries)

	// Serve static files
	r.Get("/dashboard.html", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "/home/1519995555/go/src/trial-homework/internal/web/dashboard.html")
	})
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard.html", http.StatusFound)
	})

	log.Printf("Server starting on port %s", port)

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-quit
		log.Println("Shutting down server...")

		// Stop background metrics aggregation thread
		close(stopChan)
		log.Println("Background metrics aggregation thread stopped")

		// Give running operations time to complete
		time.Sleep(2 * time.Second)
	}()

	if err := http.ListenAndServe(":"+port, r); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Server failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
