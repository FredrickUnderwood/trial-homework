package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// BidRequest represents a bid request payload
type BidRequest struct {
	DeviceID   string  `json:"device_id"`
	AppID      string  `json:"app_id"`
	CampaignID string  `json:"campaign_id"`
	BidFloor   float64 `json:"bid_floor"`
	Timestamp  int64   `json:"timestamp"`
	Country    string  `json:"country"`
	OS         string  `json:"os"`
	DeviceType string  `json:"device_type"`
	AdSlotID   string  `json:"ad_slot_id"`
	// Corner case fields
	InvalidDeviceID bool `json:"invalid_device_id,omitempty"` // empty or malformed
	NegativeBid     bool `json:"negative_bid,omitempty"`      // negative bid floor
	ZeroBid         bool `json:"zero_bid,omitempty"`          // zero bid floor
	MissingCampaign bool `json:"missing_campaign,omitempty"`  // missing campaign_id
}

// BidResponse represents the bid response from server
type BidResponse struct {
	BidID  string  `json:"bid_id"`
	Price  float64 `json:"price"`
	Status string  `json:"status"`
}

// ImpressionRequest represents an impression/billing request
type ImpressionRequest struct {
	BidID          string `json:"bid_id"`
	ImpressionTime int64  `json:"impression_time"`
	EventType      string `json:"event_type"` // "impression" or "click"
	// Corner case fields
	InvalidBidID   bool `json:"invalid_bid_id,omitempty"`   // use non-existent bid_id
	DuplicateBidID bool `json:"duplicate_bid_id,omitempty"` // duplicate impression
	MissingBidID   bool `json:"missing_bid_id,omitempty"`   // missing bid_id
	WrongBidID     bool `json:"wrong_bid_id,omitempty"`     // random wrong bid_id
}

// ImpressionResponse represents the impression response from server
type ImpressionResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// Client represents the HTTP client for the bidding server
type Client struct {
	baseURL    string
	httpClient *http.Client
	timeout    time.Duration
}

// NewClient creates a new generator client
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		timeout: 30 * time.Second,
	}
}

// CreateBid creates a bid request via HTTP POST
func (c *Client) CreateBid(ctx context.Context, req BidRequest) (*BidResponse, error) {
	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal bid request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v1/bid", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send bid request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("bid request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var bidResp BidResponse
	if err := json.Unmarshal(body, &bidResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal bid response: %w", err)
	}

	return &bidResp, nil
}

// CreateImpression creates an impression via HTTP POST
func (c *Client) CreateImpression(ctx context.Context, req ImpressionRequest) (*ImpressionResponse, error) {
	jsonData, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal impression request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/v1/billing", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to send impression request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("impression request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var impResp ImpressionResponse
	if err := json.Unmarshal(body, &impResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal impression response: %w", err)
	}

	return &impResp, nil
}
