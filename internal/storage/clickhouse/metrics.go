package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// MetricsQuery handles ClickHouse metrics queries
type MetricsQuery struct {
	conn driver.Conn
}

// NewMetricsQuery creates a new MetricsQuery
func NewMetricsQuery(client *Client) *MetricsQuery {
	return &MetricsQuery{conn: client.conn}
}

// QueryMetrics represents a metrics query result
type QueryMetrics struct {
	Minute          string
	CampaignID      string
	AppBundle       string
	PlacementID     string
	BidCount        uint64
	ImpressionCount uint64
	ViewRate        float64
}

// QueryMetrics retrieves historical metrics from ClickHouse
func (q *MetricsQuery) QueryMetrics(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) ([]QueryMetrics, error) {
	// Build the query (use FINAL to get merged data from SummingMergeTree)
	query := `
		SELECT
			minute,
			campaign_id,
			app_bundle,
			placement_id,
			bid_count,
			impression_count,
			if(bid_count > 0, impression_count / bid_count, 0) as view_rate
		FROM metrics_minute FINAL
		WHERE minute >= ? AND minute <= ?
	`

	args := []interface{}{
		startTime.Format("200601021504"),
		endTime.Format("200601021504"),
	}

	// Add filters
	if campaignID != "" {
		query += " AND campaign_id = ?"
		args = append(args, campaignID)
	}
	if appBundle != "" {
		query += " AND app_bundle = ?"
		args = append(args, appBundle)
	}
	if placementID != "" {
		query += " AND placement_id = ?"
		args = append(args, placementID)
	}

	query += " ORDER BY minute"

	rows, err := q.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query metrics: %w", err)
	}
	defer rows.Close()

	var results []QueryMetrics
	for rows.Next() {
		var r QueryMetrics
		if err := rows.Scan(
			&r.Minute,
			&r.CampaignID,
			&r.AppBundle,
			&r.PlacementID,
			&r.BidCount,
			&r.ImpressionCount,
			&r.ViewRate,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		results = append(results, r)
	}

	return results, nil
}

// QueryAggregatedMetrics retrieves aggregated metrics from ClickHouse
func (q *MetricsQuery) QueryAggregatedMetrics(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) (*QueryMetrics, error) {
	query := `
		SELECT
			'' as minute,
			? as campaign_id,
			? as app_bundle,
			? as placement_id,
			sum(bid_count) as bid_count,
			sum(impression_count) as impression_count,
			CASE
				WHEN sum(bid_count) > 0 THEN sum(impression_count) / sum(bid_count)
				ELSE 0
			END as view_rate
		FROM metrics_minute
		WHERE minute >= ? AND minute <= ?
	`

	args := []interface{}{
		campaignID,
		appBundle,
		placementID,
		startTime.Format("200601021504"),
		endTime.Format("200601021504"),
	}

	rows, err := q.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query aggregated metrics: %w", err)
	}
	defer rows.Close()

	if rows.Next() {
		var r QueryMetrics
		if err := rows.Scan(
			&r.Minute,
			&r.CampaignID,
			&r.AppBundle,
			&r.PlacementID,
			&r.BidCount,
			&r.ImpressionCount,
			&r.ViewRate,
		); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		return &r, nil
	}

	return nil, nil
}
