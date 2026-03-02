package consumer

import (
	"bidsrv/internal/model"
	"context"
	"encoding/json"
	"log"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/twmb/franz-go/pkg/kgo"

	"bidsrv/internal/storage/clickhouse"
	"bidsrv/internal/storage/infra_redis"
)

// Consumer consumes messages from Redpanda and stores them
type Consumer struct {
	client     *kgo.Client
	chClient   *clickhouse.Client
	metrics    *infra_redis.MetricsCache
	bidCache   *infra_redis.BidCache
	consumerID string
}

// NewConsumer creates a new Consumer
func NewConsumer(brokers []string, chClient *clickhouse.Client, redisClient *redis.Client) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("bidsrv-consumer-group"),
		kgo.ConsumeTopics("bid-requests", "impressions"),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	// Initialize Redis metrics cache
	metrics, err := infra_redis.NewMetricsCache(redisClient)
	if err != nil {
		return nil, err
	}

	// Initialize Redis bid cache for lookups
	bidCache, err := infra_redis.NewBidCache(redisClient)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		client:     client,
		chClient:   chClient,
		metrics:    metrics,
		bidCache:   bidCache,
		consumerID: "consumer-1",
	}, nil
}

// Start starts the consumer loop
func (c *Consumer) Start(ctx context.Context) error {
	log.Println("Starting consumer...")

	for {
		select {
		case <-ctx.Done():
			log.Println("Consumer shutting down...")
		default:
		}

		// Poll for records with timeout
		fetches := c.client.PollFetches(ctx)

		// Process each record
		fetches.EachPartition(func(ftp kgo.FetchTopicPartition) {
			for _, record := range ftp.Records {
				switch record.Topic {
				case "bid-requests":
					if err := c.handleBidRequest(record); err != nil {
						log.Printf("Error handling bid request: %v", err)
					}
				case "impressions":
					if err := c.handleImpression(record); err != nil {
						log.Printf("Error handling impression: %v", err)
					}
				default:
					log.Printf("Unknown topic: %s", record.Topic)
				}
			}
		})

		// Commit offsets
		if err := c.client.CommitUncommittedOffsets(ctx); err != nil {
			log.Printf("Failed to commit offsets: %v", err)
		}
	}
}

func (c *Consumer) handleBidRequest(record *kgo.Record) error {
	var event model.BidEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		log.Printf("Failed to unmarshal bid event: %v", err)
		return nil // Don't retry, it's a parse error
	}

	// Store to ClickHouse
	chRecord := clickhouse.BidRecord{
		BidID:        event.BidID,
		RequestID:    event.RequestID,
		UserIDFV:     event.UserIDFV,
		CampaignID:   event.CampaignID,
		AppBundle:    event.AppBundle,
		PlacementID:  event.PlacementID,
		BidTimestamp: event.Timestamp,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.chClient.InsertBid(ctx, chRecord); err != nil {
		log.Printf("Failed to insert bid to ClickHouse: %v", err)
		return err
	}

	// Update metrics in Redis
	minute := time.Unix(event.Timestamp, 0).Format("200601021504")
	c.metrics.IncrementBidCount(ctx, minute, event.CampaignID, "", event.PlacementID)

	log.Printf("Processed bid request: bid_id=%s", event.BidID)
	return nil
}

func (c *Consumer) handleImpression(record *kgo.Record) error {
	var event model.ImpressionEvent
	if err := json.Unmarshal(record.Value, &event); err != nil {
		log.Printf("Failed to unmarshal impression event: %v", err)
		return nil // Don't retry, it's a parse error
	}

	// Get bid info from Redis for app_bundle and bid_timestamp
	var appBundle string
	var bidTimestamp int64
	var userIDFV string
	var campaignId string
	var placementId string

	if bidInfo, err := c.bidCache.GetBidInfo(context.Background(), event.BidID); err != nil {
		log.Printf("Failed to get bid info from Redis: %v", err)
	} else if bidInfo != nil {
		appBundle = bidInfo.AppBundle
		bidTimestamp = bidInfo.BidTimestamp
		userIDFV = bidInfo.UserIDFV
		campaignId = bidInfo.CampaignID
		placementId = bidInfo.PlacementID
	}

	// Store to ClickHouse
	chRecord := clickhouse.ImpressionRecord{
		BidID:               event.BidID,
		UserIDFV:            userIDFV,
		CampaignID:          campaignId,
		AppBundle:           appBundle,
		PlacementID:         placementId, // Not in impression event
		BidTimestamp:        bidTimestamp,
		ImpressionTimestamp: event.Timestamp,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.chClient.InsertImpression(ctx, chRecord); err != nil {
		log.Printf("Failed to insert impression to ClickHouse: %v", err)
		return err
	}

	// Update metrics in Redis
	minute := strconv.FormatInt(event.Timestamp, 10)
	minute = time.Unix(event.Timestamp, 0).Format("200601021504")
	c.metrics.IncrementImpressionCount(ctx, minute, campaignId, appBundle, placementId)

	log.Printf("Processed impression: bid_id=%s", event.BidID)
	return nil
}
