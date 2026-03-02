package main

import (
	"bidsrv/internal/storage/clickhouse"
	"context"
	"log"
	"net/http"
	"os"
	"strings"

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
	port := getEnv("PORT", "8082")
	baseURL := getEnv("BASE_URL", "http://localhost:8082")

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

	// 8. Init Dashboard Handler
	dashboardHandler := api.NewDashboardHandler(queryService)

	// 9. Setup Router
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/healthz", handler.HandleHealthz)
	r.Post("/v1/bid", handler.HandleBid)
	r.Post("/v1/billing", handler.HandleBilling)

	// Dashboard routes
	r.Get("/dashboard", dashboardHandler.HandleDashboard)
	r.Get("/dashboard/timeseries", dashboardHandler.HandleDashboardTimeSeries)
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/dashboard.html", http.StatusFound)
	})
	// Serve static files
	r.Get("/dashboard.html", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "internal/web/dashboard.html")
	})

	log.Printf("Server starting on port %s", port)
	if err := http.ListenAndServe(":"+port, r); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}
