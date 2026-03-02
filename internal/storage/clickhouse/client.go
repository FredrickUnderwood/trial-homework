package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Client handles ClickHouse connections
type Client struct {
	conn driver.Conn
}

// NewClient creates a new ClickHouse client
func NewClient(addr string) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: "bidsrv",
		},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to clickhouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping clickhouse: %w", err)
	}

	return &Client{conn: conn}, nil
}

// BidRecord represents a bid record for ClickHouse
type BidRecord struct {
	BidID        string
	RequestID    string
	UserIDFV     string
	CampaignID   string
	AppBundle    string
	PlacementID  string
	BidTimestamp int64
}

// InsertBid inserts a bid record into ClickHouse
func (c *Client) InsertBid(ctx context.Context, record BidRecord) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO bids")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	if err := batch.Append(
		record.BidID,
		record.RequestID,
		record.UserIDFV,
		record.CampaignID,
		record.AppBundle,
		record.PlacementID,
		record.BidTimestamp,
	); err != nil {
		return fmt.Errorf("failed to append to batch: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// ImpressionRecord represents an impression record for ClickHouse
type ImpressionRecord struct {
	BidID               string
	UserIDFV            string
	CampaignID          string
	AppBundle           string
	PlacementID         string
	BidTimestamp        int64
	ImpressionTimestamp int64
}

// InsertImpression inserts an impression record into ClickHouse
func (c *Client) InsertImpression(ctx context.Context, record ImpressionRecord) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO impressions")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	if err := batch.Append(
		record.BidID,
		record.UserIDFV,
		record.CampaignID,
		record.AppBundle,
		record.PlacementID,
		record.BidTimestamp,
		record.ImpressionTimestamp,
	); err != nil {
		return fmt.Errorf("failed to append to batch: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// MetricsRecord represents a metrics record for ClickHouse
type MetricsRecord struct {
	Minute          string
	CampaignID      string
	AppBundle       string
	PlacementID     string
	BidCount        uint64
	ImpressionCount uint64
	ViewRate        float32
}

// InsertMetrics inserts a metrics record into ClickHouse
func (c *Client) InsertMetrics(ctx context.Context, record MetricsRecord) error {
	batch, err := c.conn.PrepareBatch(ctx, "INSERT INTO metrics_minute")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	if err := batch.Append(
		record.Minute,
		record.CampaignID,
		record.AppBundle,
		record.PlacementID,
		record.BidCount,
		record.ImpressionCount,
		record.ViewRate,
	); err != nil {
		return fmt.Errorf("failed to append to batch: %w", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}
