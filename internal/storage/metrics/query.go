package metrics

import (
	"context"
	"fmt"
	"time"

	"bidsrv/internal/storage/clickhouse"
	"bidsrv/internal/storage/infra_redis"

	"github.com/go-redis/redis/v8"
)

// QueryService handles metrics queries from both Redis and ClickHouse
type QueryService struct {
	chClient *clickhouse.Client
	chQuery  *clickhouse.MetricsQuery
	metrics  *infra_redis.MetricsCache
}

// NewQueryService creates a new QueryService
func NewQueryService(chClient *clickhouse.Client, redisClient *redis.Client) (*QueryService, error) {

	chQuery := clickhouse.NewMetricsQuery(chClient)

	metrics, err := infra_redis.NewMetricsCache(redisClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics cache: %w", err)
	}

	return &QueryService{
		chClient: chClient,
		chQuery:  chQuery,
		metrics:  metrics,
	}, nil
}

// QueryResult represents the result of a metrics query
type QueryResult struct {
	StartTime       time.Time `json:"start_time"`
	EndTime         time.Time `json:"end_time"`
	CampaignID      string    `json:"campaign_id,omitempty"`
	AppBundle       string    `json:"app_bundle,omitempty"`
	PlacementID     string    `json:"placement_id,omitempty"`
	BidCount        uint64    `json:"bid_count"`
	ImpressionCount uint64    `json:"impression_count"`
	ViewRate        float64   `json:"view_rate"`
	Source          string    `json:"source"` // "infra_redis", "clickhouse", or "merged"
}

// Query queries metrics for the given time range
// It uses Redis for data less than 1 hour old, and ClickHouse for older data
func (s *QueryService) Query(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) ([]QueryResult, error) {
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)

	var recentResults []QueryResult
	var historicalResults []QueryResult

	// Split the time range into recent (< 1 hour) and historical (>= 1 hour)
	if startTime.After(oneHourAgo) {
		// All data is recent - query from Redis
		recentResults, err := s.queryFromRedis(ctx, startTime, endTime, campaignID, appBundle, placementID)
		if err != nil {
			return nil, fmt.Errorf("failed to query from infra_redis: %w", err)
		}
		return recentResults, nil
	}

	if endTime.Before(oneHourAgo) {
		// All data is historical - query from ClickHouse
		historicalResults, err := s.queryFromClickHouse(ctx, startTime, endTime, campaignID, appBundle, placementID)
		if err != nil {
			return nil, fmt.Errorf("failed to query from clickhouse: %w", err)
		}
		return historicalResults, nil
	}

	// Time range spans both recent and historical - query both and merge
	recentStart := oneHourAgo
	historicalEnd := oneHourAgo

	// Query recent data from Redis
	recentResults, err := s.queryFromRedis(ctx, recentStart, endTime, campaignID, appBundle, placementID)
	if err != nil {
		return nil, fmt.Errorf("failed to query from infra_redis: %w", err)
	}

	// Query historical data from ClickHouse
	historicalResults, err = s.queryFromClickHouse(ctx, startTime, historicalEnd, campaignID, appBundle, placementID)
	if err != nil {
		return nil, fmt.Errorf("failed to query from clickhouse: %w", err)
	}

	// Merge results
	return s.mergeResults(recentResults, historicalResults), nil
}

// QueryAggregated queries aggregated metrics for the given time range
func (s *QueryService) QueryAggregated(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) (*QueryResult, error) {
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)

	// If all data is recent, query from Redis
	if startTime.After(oneHourAgo) {
		return s.queryAggregatedFromRedis(ctx, startTime, endTime, campaignID, appBundle, placementID)
	}

	// If all data is historical, query from ClickHouse
	if endTime.Before(oneHourAgo) {
		return s.queryAggregatedFromClickHouse(ctx, startTime, endTime, campaignID, appBundle, placementID)
	}

	// Query both and merge
	recentStart := oneHourAgo
	historicalEnd := oneHourAgo

	recent, err := s.queryAggregatedFromRedis(ctx, recentStart, endTime, campaignID, appBundle, placementID)
	if err != nil {
		return nil, err
	}

	historical, err := s.queryAggregatedFromClickHouse(ctx, startTime, historicalEnd, campaignID, appBundle, placementID)
	if err != nil {
		return nil, err
	}

	// Merge aggregated results
	return &QueryResult{
		StartTime:       startTime,
		EndTime:         endTime,
		CampaignID:      campaignID,
		AppBundle:       appBundle,
		PlacementID:     placementID,
		BidCount:        recent.BidCount + historical.BidCount,
		ImpressionCount: recent.ImpressionCount + historical.ImpressionCount,
		ViewRate:        calculateViewRate(recent.BidCount+historical.BidCount, recent.ImpressionCount+historical.ImpressionCount),
		Source:          "merged",
	}, nil
}

func (s *QueryService) queryFromRedis(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) ([]QueryResult, error) {
	// Iterate through each minute in the time range
	var results []QueryResult

	for t := startTime; t.Before(endTime); t = t.Add(1 * time.Minute) {
		minute := t.Format("200601021504")

		metricsData, err := s.metrics.GetMetrics(ctx, minute, campaignID, appBundle, placementID)
		if err != nil {
			return nil, err
		}

		// Only add if there's data
		if metricsData.BidCount > 0 || metricsData.ImpressionCount > 0 {
			results = append(results, QueryResult{
				StartTime:       t,
				EndTime:         t.Add(1 * time.Minute),
				CampaignID:      campaignID,
				AppBundle:       appBundle,
				PlacementID:     placementID,
				BidCount:        metricsData.BidCount,
				ImpressionCount: metricsData.ImpressionCount,
				ViewRate:        float64(metricsData.ViewRate),
				Source:          "infra_redis",
			})
		}
	}

	return results, nil
}

func (s *QueryService) queryAggregatedFromRedis(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) (*QueryResult, error) {
	var totalBidCount, totalImpressionCount uint64

	// Iterate through each minute in the time range
	for t := startTime; t.Before(endTime); t = t.Add(1 * time.Minute) {
		minute := t.Format("200601021504")

		metricsData, err := s.metrics.GetMetrics(ctx, minute, campaignID, appBundle, placementID)
		if err != nil {
			return nil, err
		}

		totalBidCount += metricsData.BidCount
		totalImpressionCount += metricsData.ImpressionCount
	}

	return &QueryResult{
		StartTime:       startTime,
		EndTime:         endTime,
		CampaignID:      campaignID,
		AppBundle:       appBundle,
		PlacementID:     placementID,
		BidCount:        totalBidCount,
		ImpressionCount: totalImpressionCount,
		ViewRate:        calculateViewRate(totalBidCount, totalImpressionCount),
		Source:          "infra_redis",
	}, nil
}

func (s *QueryService) queryFromClickHouse(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) ([]QueryResult, error) {
	chResults, err := s.chQuery.QueryMetrics(ctx, startTime, endTime, campaignID, appBundle, placementID)
	if err != nil {
		return nil, err
	}

	var results []QueryResult
	for _, r := range chResults {
		t, err := time.Parse("200601021504", r.Minute)
		if err != nil {
			continue
		}

		results = append(results, QueryResult{
			StartTime:       t,
			EndTime:         t.Add(1 * time.Minute),
			CampaignID:      r.CampaignID,
			AppBundle:       r.AppBundle,
			PlacementID:     r.PlacementID,
			BidCount:        r.BidCount,
			ImpressionCount: r.ImpressionCount,
			ViewRate:        r.ViewRate,
			Source:          "clickhouse",
		})
	}

	return results, nil
}

func (s *QueryService) queryAggregatedFromClickHouse(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) (*QueryResult, error) {
	metrics, err := s.chQuery.QueryAggregatedMetrics(ctx, startTime, endTime, campaignID, appBundle, placementID)
	if err != nil {
		return nil, err
	}

	if metrics == nil {
		return &QueryResult{
			StartTime:       startTime,
			EndTime:         endTime,
			CampaignID:      campaignID,
			AppBundle:       appBundle,
			PlacementID:     placementID,
			BidCount:        0,
			ImpressionCount: 0,
			ViewRate:        0,
			Source:          "clickhouse",
		}, nil
	}

	return &QueryResult{
		StartTime:       startTime,
		EndTime:         endTime,
		CampaignID:      metrics.CampaignID,
		AppBundle:       metrics.AppBundle,
		PlacementID:     metrics.PlacementID,
		BidCount:        metrics.BidCount,
		ImpressionCount: metrics.ImpressionCount,
		ViewRate:        metrics.ViewRate,
		Source:          "clickhouse",
	}, nil
}

func (s *QueryService) mergeResults(recentResults, historicalResults []QueryResult) []QueryResult {
	// Combine results - in a real implementation, we'd aggregate by minute
	var merged []QueryResult
	merged = append(merged, historicalResults...)
	merged = append(merged, recentResults...)
	return merged
}

func calculateViewRate(bidCount, impressionCount uint64) float64 {
	if bidCount == 0 {
		return 0
	}
	return float64(impressionCount) / float64(bidCount)
}
