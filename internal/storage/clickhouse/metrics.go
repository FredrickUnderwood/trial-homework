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
	// Build the base query
	query := `
		SELECT
			minute,
			campaign_id,
			app_bundle,
			placement_id,
			sum(bid_count) as bid_count,
			sum(impression_count) as impression_count,
			if(sum(bid_count) > 0, sum(impression_count) / sum(bid_count), 0) as view_rate
		FROM metrics_minute
		WHERE minute >= ? AND minute <= ?
	`

	args := []interface{}{
		startTime.Format("200601021504"),
		endTime.Format("200601021504"),
	}

	// Add filters only if they are provided (non-empty)
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

	// Group by all dimensions
	query += " GROUP BY minute, campaign_id, app_bundle, placement_id ORDER BY minute"

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
// When no filters are provided, it returns the total aggregated across all data
func (q *MetricsQuery) QueryAggregatedMetrics(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) (*QueryMetrics, error) {
	// First query: get total bid_count and impression_count using subquery
	innerQuery := `
		SELECT
			sum(bid_count) as total_bid_count,
			sum(impression_count) as total_impression_count
		FROM metrics_minute
		WHERE minute >= ? AND minute <= ?
	`

	args := []interface{}{
		startTime.Format("200601021504"),
		endTime.Format("200601021504"),
	}

	// Add filters only if they are provided (non-empty)
	if campaignID != "" {
		innerQuery += " AND campaign_id = ?"
		args = append(args, campaignID)
	}
	if appBundle != "" {
		innerQuery += " AND app_bundle = ?"
		args = append(args, appBundle)
	}
	if placementID != "" {
		innerQuery += " AND placement_id = ?"
		args = append(args, placementID)
	}

	// Outer query: calculate view_rate
	query := fmt.Sprintf(`
		SELECT
			'' as minute,
			? as campaign_id,
			? as app_bundle,
			? as placement_id,
			total_bid_count,
			total_impression_count,
			if(total_bid_count > 0, total_impression_count / total_bid_count, 0) as view_rate
		FROM (%s)
	`, innerQuery)

	outerArgs := []interface{}{
		campaignID,
		appBundle,
		placementID,
	}
	outerArgs = append(outerArgs, args...)

	rows, err := q.conn.Query(ctx, query, outerArgs...)
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

	// No data found, return empty result
	return &QueryMetrics{
		CampaignID:      campaignID,
		AppBundle:       appBundle,
		PlacementID:     placementID,
		BidCount:        0,
		ImpressionCount: 0,
		ViewRate:        0,
	}, nil
}
