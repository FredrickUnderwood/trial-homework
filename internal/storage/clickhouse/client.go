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

var loc, _ = time.LoadLocation("Asia/Shanghai")

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
		time.Now().In(loc),
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
		time.Now().In(loc),
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

// AggregateMetrics aggregates data from bids and impressions tables into metrics_minute
func (c *Client) AggregateMetrics(ctx context.Context, minute string) error {
	query := fmt.Sprintf(`
		INSERT INTO metrics_minute
		SELECT
			'%s' as minute,
			campaign_id,
			app_bundle,
			placement_id,
			count(*) as bid_count,
			sum(if(i.bid_id IS NOT NULL, 1, 0)) as impression_count,
			if(count(*) > 0, sum(if(i.bid_id IS NOT NULL, 1, 0)) / count(*), 0) as view_rate
		FROM bids b
		LEFT JOIN impressions i ON b.bid_id = i.bid_id
		WHERE 
			formatDateTime(toDateTime(b.bid_timestamp), '%%Y%%m%%d%%H%%i') = '%s'
		GROUP BY campaign_id, app_bundle, placement_id
	`, minute, minute)

	err := c.conn.AsyncInsert(ctx, query, false)
	if err != nil {
		return fmt.Errorf("failed to aggregate metrics: %w", err)
	}

	return nil
}

// GetMinutesToAggregate returns minutes that need to be aggregated
func (c *Client) GetMinutesToAggregate(ctx context.Context) ([]string, error) {
	query := `
		SELECT DISTINCT formatDateTime(toDateTime(bid_timestamp), '%Y%m%d%H%i') as minute
		FROM bids
		WHERE formatDateTime(toDateTime(bid_timestamp), '%Y%m%d%H%i') NOT IN (SELECT DISTINCT minute FROM metrics_minute)
		ORDER BY minute
	`

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get minutes to aggregate: %w", err)
	}
	defer rows.Close()

	var minutes []string
	for rows.Next() {
		var minute string
		if err := rows.Scan(&minute); err != nil {
			return nil, fmt.Errorf("failed to scan minute: %w", err)
		}
		minutes = append(minutes, minute)
	}

	return minutes, nil
}
