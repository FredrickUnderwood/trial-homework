package api

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"time"

	"bidsrv/internal/storage/metrics"
)

// DashboardHandler handles dashboard API requests
type DashboardHandler struct {
	queryService *metrics.QueryService
}

// NewDashboardHandler creates a new DashboardHandler
func NewDashboardHandler(qs *metrics.QueryService) *DashboardHandler {
	return &DashboardHandler{
		queryService: qs,
	}
}

// DashboardQueryParams represents the query parameters for dashboard
type DashboardQueryParams struct {
	CampaignID  string
	AppBundle   string
	PlacementID string
	StartTime   time.Time
	EndTime     time.Time
}

// HandleDashboard handles GET /dashboard
func (h *DashboardHandler) HandleDashboard(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	params, err := h.parseQueryParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Query aggregated metrics
	result, err := h.queryService.QueryAggregated(
		r.Context(),
		params.StartTime,
		params.EndTime,
		params.CampaignID,
		params.AppBundle,
		params.PlacementID,
	)
	if err != nil {
		log.Printf("error querying metrics: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(result)
}

// HandleDashboardTimeSeries handles GET /dashboard/timeseries
func (h *DashboardHandler) HandleDashboardTimeSeries(w http.ResponseWriter, r *http.Request) {
	// Parse query parameters
	params, err := h.parseQueryParams(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Query time series metrics
	results, err := h.queryService.Query(
		r.Context(),
		params.StartTime,
		params.EndTime,
		params.CampaignID,
		params.AppBundle,
		params.PlacementID,
	)
	if err != nil {
		log.Printf("error querying time series metrics: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Return JSON response
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(results)
}

func (h *DashboardHandler) parseQueryParams(r *http.Request) (*DashboardQueryParams, error) {
	params := &DashboardQueryParams{
		CampaignID:  r.URL.Query().Get("campaign_id"),
		AppBundle:   r.URL.Query().Get("app_bundle"),
		PlacementID: r.URL.Query().Get("placement_id"),
	}

	// Parse time range - default to last hour
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)

	if startTimeStr := r.URL.Query().Get("start_time"); startTimeStr != "" {
		// Try parsing as Unix timestamp
		if t, err := strconv.ParseInt(startTimeStr, 10, 64); err == nil {
			params.StartTime = time.Unix(t, 0)
		} else {
			// Try parsing as RFC3339
			if t, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
				params.StartTime = t
			} else {
				return nil, &ParseError{Field: "start_time", Message: "invalid format"}
			}
		}
	} else {
		params.StartTime = oneHourAgo
	}

	if endTimeStr := r.URL.Query().Get("end_time"); endTimeStr != "" {
		// Try parsing as Unix timestamp
		if t, err := strconv.ParseInt(endTimeStr, 10, 64); err == nil {
			params.EndTime = time.Unix(t, 0)
		} else {
			// Try parsing as RFC3339
			if t, err := time.Parse(time.RFC3339, endTimeStr); err == nil {
				params.EndTime = t
			} else {
				return nil, &ParseError{Field: "end_time", Message: "invalid format"}
			}
		}
	} else {
		params.EndTime = now
	}

	// Validate time range
	if params.StartTime.After(params.EndTime) {
		return nil, &ParseError{Field: "start_time", Message: "must be before end_time"}
	}

	return params, nil
}

// ParseError represents a query parameter parsing error
type ParseError struct {
	Field   string
	Message string
}

func (e *ParseError) Error() string {
	return e.Field + ": " + e.Message
}
