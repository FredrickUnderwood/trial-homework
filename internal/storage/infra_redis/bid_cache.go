package infra_redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// BidCache stores bid information in Redis for later retrieval during billing
type BidCache struct {
	client *redis.Client
}

// BidInfo represents the bid information stored in Redis
type BidInfo struct {
	CampaignID  string `json:"campaign_id"`
	AppBundle   string `json:"app_bundle"`
	PlacementID string `json:"placement_id"`
	UserIDFV    string `json:"user_idfv"`
}

// NewBidCache creates a new BidCache instance
func NewBidCache(client *redis.Client) (*BidCache, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("infra_redis ping failed: %w", err)
	}

	return &BidCache{client: client}, nil
}

// SetBidInfo stores bid information in Redis with 24 hour TTL
func (c *BidCache) SetBidInfo(ctx context.Context, bidID string, info BidInfo) error {
	key := fmt.Sprintf("bid:%s", bidID)
	ttl := 24 * time.Hour

	data, err := json.Marshal(info)
	if err != nil {
		return fmt.Errorf("failed to marshal bid info: %w", err)
	}

	if err := c.client.Set(ctx, key, data, ttl).Err(); err != nil {
		return fmt.Errorf("failed to set bid info in infra_redis: %w", err)
	}

	return nil
}

// GetBidInfo retrieves bid information from Redis
func (c *BidCache) GetBidInfo(ctx context.Context, bidID string) (*BidInfo, error) {
	key := fmt.Sprintf("bid:%s", bidID)

	data, err := c.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get bid info from infra_redis: %w", err)
	}

	var info BidInfo
	if err := json.Unmarshal([]byte(data), &info); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bid info: %w", err)
	}

	return &info, nil
}
