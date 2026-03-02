package infra_redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// MetricsCache stores metrics data in Redis
type MetricsCache struct {
	client *redis.Client
}

// MetricsData represents the metrics data for a specific minute
type MetricsData struct {
	BidCount        uint64  `json:"bid_count"`
	ImpressionCount uint64  `json:"impression_count"`
	ViewRate        float32 `json:"view_rate"`
}

// NewMetricsCache creates a new MetricsCache instance
func NewMetricsCache(client *redis.Client) (*MetricsCache, error) {

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("infra_redis ping failed: %w", err)
	}

	return &MetricsCache{client: client}, nil
}

// getKey returns the Redis key for a given minute and dimensions
func (c *MetricsCache) getKey(minute, campaignID, appBundle, placementID string) string {
	return fmt.Sprintf("metrics:minute:%s:%s:%s:%s", minute, campaignID, appBundle, placementID)
}

// IncrementBidCount increments the bid count for a given minute and dimensions
func (c *MetricsCache) IncrementBidCount(ctx context.Context, minute, campaignID, appBundle, placementID string) error {
	key := c.getKey(minute, campaignID, appBundle, placementID)
	ttl := 2 * time.Hour

	// Use a transaction to ensure atomicity
	pipe := c.client.Pipeline()

	// Increment bid count
	pipe.HIncrBy(ctx, key, "bid_count", 1)

	// Set TTL if not already set
	pipe.Expire(ctx, key, ttl)

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to increment bid count: %w", err)
	}

	return nil
}

// IncrementImpressionCount increments the impression count for a given minute and dimensions
func (c *MetricsCache) IncrementImpressionCount(ctx context.Context, minute, campaignID, appBundle, placementID string) error {
	key := c.getKey(minute, campaignID, appBundle, placementID)
	ttl := 2 * time.Hour

	// First, get current values
	data, err := c.GetMetrics(ctx, minute, campaignID, appBundle, placementID)
	if err != nil {
		return err
	}

	// Increment impression count
	data.ImpressionCount++

	// Calculate view rate
	if data.BidCount > 0 {
		data.ViewRate = float32(data.ImpressionCount) / float32(data.BidCount)
	}

	// Store updated data
	jsonData, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal metrics data: %w", err)
	}

	if err := c.client.Set(ctx, key, jsonData, ttl).Err(); err != nil {
		return fmt.Errorf("failed to set metrics data: %w", err)
	}

	return nil
}

// GetMetrics retrieves metrics for a given minute and dimensions
func (c *MetricsCache) GetMetrics(ctx context.Context, minute, campaignID, appBundle, placementID string) (*MetricsData, error) {
	key := c.getKey(minute, campaignID, appBundle, placementID)

	data, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		// Return empty metrics if not found
		return &MetricsData{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get metrics from infra_redis: %w", err)
	}

	var metrics MetricsData
	if err := json.Unmarshal([]byte(data), &metrics); err != nil {
		return nil, fmt.Errorf("failed to unmarshal metrics data: %w", err)
	}

	return &metrics, nil
}

// GetMetricsByFilters retrieves all metrics for a given minute with optional filters
// If campaignID, appBundle, or placementID is empty, it acts as a wildcard
func (c *MetricsCache) GetMetricsByFilters(ctx context.Context, minute, campaignID, appBundle, placementID string) ([]MetricsData, error) {
	pattern := fmt.Sprintf("metrics:minute:%s:%s:%s:%s",
		minute,
		wildcard(campaignID),
		wildcard(appBundle),
		wildcard(placementID),
	)

	keys, err := c.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get keys: %w", err)
	}

	if len(keys) == 0 {
		return []MetricsData{}, nil
	}

	// Get all values
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get values: %w", err)
	}

	var results []MetricsData
	for _, v := range values {
		if v == nil {
			continue
		}
		var metrics MetricsData
		if err := json.Unmarshal([]byte(v.(string)), &metrics); err != nil {
			continue
		}
		results = append(results, metrics)
	}

	return results, nil
}

// wildcard returns "*" if the input is empty, otherwise returns the input
func wildcard(s string) string {
	if s == "" {
		return "*"
	}
	return s
}
