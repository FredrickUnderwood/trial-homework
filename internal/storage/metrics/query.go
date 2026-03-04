package metrics

import (
	"context"
	"time"

	"bidsrv/internal/storage/clickhouse"

	"github.com/go-redis/redis/v8"
)

// QueryService handles metrics queries from ClickHouse
type QueryService struct {
	chClient *clickhouse.Client
	chQuery  *clickhouse.MetricsQuery
}

// NewQueryService creates a new QueryService
func NewQueryService(chClient *clickhouse.Client, redisClient *redis.Client) (*QueryService, error) {
	chQuery := clickhouse.NewMetricsQuery(chClient)

	return &QueryService{
		chClient: chClient,
		chQuery:  chQuery,
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
	Source          string    `json:"source"` // always "clickhouse"
}

// Query queries metrics for the given time range
// It queries from ClickHouse for all data
func (s *QueryService) Query(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) ([]QueryResult, error) {
	// Query time series data from ClickHouse
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

// QueryAggregated queries aggregated metrics for the given time range
func (s *QueryService) QueryAggregated(ctx context.Context, startTime, endTime time.Time, campaignID, appBundle, placementID string) (*QueryResult, error) {
	// Query aggregated data from ClickHouse
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
