package generator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"time"
)

// BidRequest represents a bid request payload (matches server API)
type BidRequest struct {
	UserIDFV    string `json:"user_idfv"`
	AppBundle   string `json:"app_bundle"`
	PlacementID string `json:"placement_id"`
	Timestamp   int64  `json:"timestamp"`
	// Additional fields for campaign selection (optional)
	Country string `json:"country,omitempty"`
	OS      string `json:"os,omitempty"`
	// Corner case flags (not sent to server, used internally)
	IsCornerCase bool `json:"-"`
}

// BidResponse represents the bid response from server
type BidResponse struct {
	BidID       string  `json:"bid_id"`
	RequestID   string  `json:"request_id"`
	CampaignID  string  `json:"campaign_id"`
	CreativeURL string  `json:"creative_url"`
	BillingURL  string  `json:"billing_url"`
	Price       float64 `json:"price,omitempty"`
	Status      string  `json:"status,omitempty"`
}

// ImpressionRequest represents an impression/billing request
type ImpressionRequest struct {
	BidID     string `json:"bid_id"`
	Timestamp int64  `json:"timestamp"`
}

// ImpressionResponse represents the impression response from server
type ImpressionResponse struct {
	Status string `json:"status"`
}

// Device IDs for normal requests
var validDeviceIDs = []string{
	"device_a1b2c3d4e5f6",
	"device_9876543210ab",
	"device_xyz123456789",
	"device_abc123def456",
	"device_111222333444",
	"device_android_001",
	"device_android_002",
	"device_ios_001",
	"device_ios_002",
}

// Invalid device IDs for corner cases
var invalidDeviceIDs = []string{
	"",
	"   ",
	"invalid!@#$%",
	"NULL",
	"unknown",
}

// App Bundles - realistic mobile app bundle IDs
var appBundles = []string{
	// Social apps
	"com.facebook.katana",
	"com.instagram.android",
	"com.twitter.android",
	"com.snapchat.android",
	"com.tiktok",
	// Games
	"com.supercell.clashofclans",
	"com.king.candycrushsaga",
	"com.rovio.angrybirds",
	"com.gameloft.android.ANMP.GloftA8HM",
	"com.mojang.minecraftpe",
	// Shopping
	"com.amazon.mShop.android.shopping",
	"com.ebay.mobile",
	"com.walmart.android",
	"com.target",
	// Entertainment
	"com.netflix.mediaclient",
	"com.google.android.youtube",
	"com.spotify.music",
	"com.disney.disneyplus",
	// News & Reading
	"com.flipboard.app",
	"com.twitter.android",
	"com.reddit.frontpage",
	// Utilities
	"com.whatsapp",
	"com.viber.voip",
	"com.linecorp.LINEarroid",
	"com.skype.raider",
	// Productivity
	"com.microsoft.office.word",
	"com.google.android.apps.docs",
	"com.slack",
	"com.zoom.us",
}

// Placement IDs
var placementIDs = []string{
	"banner_top",
	"banner_bottom",
	"interstitial",
	"rewarded",
	"native",
	"splash",
}

// Countries
var countries = []string{
	"US", "UK", "CA", "AU", "DE", "FR", "JP", "KR", "SG", "IN",
}

// OS types
var osTypes = []string{
	"iOS", "Android", "Windows", "macOS",
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		return nil, fmt.Errorf("bid request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Handle NoContent (no bid fill)
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
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

	// Handle empty response body (server returns 200 OK with no body)
	if len(body) == 0 {
		return &ImpressionResponse{Status: "ok"}, nil
	}

	var impResp ImpressionResponse
	if err := json.Unmarshal(body, &impResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal impression response: %w", err)
	}

	return &impResp, nil
}

// GenerateBidRequest generates a bid request with optional corner cases
func GenerateBidRequest(isCornerCase bool) BidRequest {
	if isCornerCase && rand.Float32() < 0.3 {
		return generateCornerCaseBid()
	}
	return generateNormalBid()
}

// generateNormalBid generates a normal bid request
func generateNormalBid() BidRequest {
	return BidRequest{
		UserIDFV:    validDeviceIDs[rand.Intn(len(validDeviceIDs))],
		AppBundle:   appBundles[rand.Intn(len(appBundles))],
		PlacementID: placementIDs[rand.Intn(len(placementIDs))],
		Timestamp:   time.Now().UnixMilli(),
		Country:     countries[rand.Intn(len(countries))],
		OS:          osTypes[rand.Intn(len(osTypes))],
	}
}

// generateCornerCaseBid generates various corner case bid requests
func generateCornerCaseBid() BidRequest {
	caseType := rand.Intn(5)
	req := generateNormalBid()
	req.IsCornerCase = true

	switch caseType {
	case 0:
		// Invalid/empty user_idfv
		req.UserIDFV = invalidDeviceIDs[rand.Intn(len(invalidDeviceIDs))]
	case 1:
		// Zero timestamp
		req.Timestamp = 0
	case 2:
		// Very old timestamp (1 year ago)
		req.Timestamp = time.Now().AddDate(-1, 0, 0).UnixMilli()
	case 3:
		// Future timestamp
		req.Timestamp = time.Now().AddDate(1, 0, 0).UnixMilli()
	case 4:
		// Missing app_bundle (empty)
		req.AppBundle = ""
	}

	return req
}

// GenerateImpressionRequest generates an impression request
func GenerateImpressionRequest(bidID string, isCornerCase bool) ImpressionRequest {
	if isCornerCase && rand.Float32() < 0.3 {
		return generateCornerCaseImpression(bidID)
	}
	return generateNormalImpression(bidID)
}

// generateNormalImpression generates a normal impression request
func generateNormalImpression(bidID string) ImpressionRequest {
	return ImpressionRequest{
		BidID:     bidID,
		Timestamp: time.Now().UnixMilli(),
	}
}

// generateCornerCaseImpression generates various corner case impression requests
func generateCornerCaseImpression(bidID string) ImpressionRequest {
	caseType := rand.Intn(4)
	req := ImpressionRequest{
		Timestamp: time.Now().UnixMilli(),
	}

	switch caseType {
	case 0:
		// Invalid bid ID - random non-existent ID
		req.BidID = fmt.Sprintf("invalid_%d", rand.Int63())
	case 1:
		// Duplicate bid ID (same bid used twice) - will be handled by idempotency
		req.BidID = bidID
	case 2:
		// Missing bid ID
		req.BidID = ""
	case 3:
		// Wrong bid ID format
		req.BidID = fmt.Sprintf("bid_wrong_%d", rand.Int63())
	}

	return req
}
