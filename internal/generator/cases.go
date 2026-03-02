package generator

import (
	"fmt"
	"math/rand"
	"time"
)

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

// App IDs
var appIDs = []string{
	"com.example.app",
	"com.game.mygame",
	"com.social.app",
	"com.news.reader",
	"com.video.player",
	"app_commerce_001",
	"app_fitness_001",
}

// Campaign IDs
var campaignIDs = []string{
	"camp_001",
	"camp_002",
	"camp_003",
	"camp_004",
	"camp_005",
	"camp_premium",
	"camp_standard",
	"camp_budget",
}

// Countries
var countries = []string{
	"US",
	"UK",
	"CA",
	"AU",
	"DE",
	"FR",
	"JP",
	"KR",
	"SG",
	"IN",
}

// OS types
var osTypes = []string{
	"iOS",
	"Android",
	"Windows",
	"macOS",
}

// Device types
var deviceTypes = []string{
	"mobile",
	"tablet",
	"desktop",
	"tv",
}

// Ad slot IDs
var adSlotIDs = []string{
	"slot_banner_top",
	"slot_banner_bottom",
	"slot_interstitial",
	"slot_rewarded",
	"slot_native",
	"slot_splash",
}

// GenerateBidRequest generates a bid request with optional corner cases
func GenerateBidRequest(isCornerCase bool) BidRequest {
	// If corner case, decide which type
	if isCornerCase && rand.Float32() < 0.5 {
		return generateCornerCaseBid()
	}
	return generateNormalBid()
}

// generateNormalBid generates a normal bid request
func generateNormalBid() BidRequest {
	return BidRequest{
		DeviceID:   validDeviceIDs[rand.Intn(len(validDeviceIDs))],
		AppID:      appIDs[rand.Intn(len(appIDs))],
		CampaignID: campaignIDs[rand.Intn(len(campaignIDs))],
		BidFloor:   randFloat(0.1, 10.0),
		Timestamp:  time.Now().UnixMilli(),
		Country:    countries[rand.Intn(len(countries))],
		OS:         osTypes[rand.Intn(len(osTypes))],
		DeviceType: deviceTypes[rand.Intn(len(deviceTypes))],
		AdSlotID:   adSlotIDs[rand.Intn(len(adSlotIDs))],
	}
}

// generateCornerCaseBid generates various corner case bid requests
func generateCornerCaseBid() BidRequest {
	caseType := rand.Intn(5)
	req := generateNormalBid()

	switch caseType {
	case 0:
		// Invalid/empty device ID
		req.InvalidDeviceID = true
		req.DeviceID = invalidDeviceIDs[rand.Intn(len(invalidDeviceIDs))]
	case 1:
		// Negative bid floor
		req.NegativeBid = true
		req.BidFloor = -randFloat(0.1, 5.0)
	case 2:
		// Zero bid floor
		req.ZeroBid = true
		req.BidFloor = 0
	case 3:
		// Missing campaign ID
		req.MissingCampaign = true
		req.CampaignID = ""
	case 4:
		// Very high bid floor
		req.BidFloor = randFloat(100.0, 1000.0)
	}

	return req
}

// GenerateImpressionRequest generates an impression request
func GenerateImpressionRequest(bidID string, isCornerCase bool) ImpressionRequest {
	// If corner case, decide which type
	if isCornerCase && rand.Float32() < 0.3 {
		return generateCornerCaseImpression(bidID)
	}
	return generateNormalImpression(bidID)
}

// generateNormalImpression generates a normal impression request
func generateNormalImpression(bidID string) ImpressionRequest {
	return ImpressionRequest{
		BidID:          bidID,
		ImpressionTime: time.Now().UnixMilli(),
		EventType:      "impression",
	}
}

// generateCornerCaseImpression generates various corner case impression requests
func generateCornerCaseImpression(bidID string) ImpressionRequest {
	caseType := rand.Intn(4)
	req := ImpressionRequest{
		ImpressionTime: time.Now().UnixMilli(),
		EventType:      "impression",
	}

	switch caseType {
	case 0:
		// Invalid bid ID - random non-existent ID
		req.InvalidBidID = true
		req.BidID = fmt.Sprintf("invalid_%d", rand.Int63())
	case 1:
		// Duplicate bid ID (same bid used twice)
		req.DuplicateBidID = true
		req.BidID = bidID
	case 2:
		// Missing bid ID
		req.MissingBidID = true
		req.BidID = ""
	case 3:
		// Wrong bid ID format
		req.WrongBidID = true
		req.BidID = fmt.Sprintf("bid_wrong_%d", rand.Int63())
	}

	return req
}

// randFloat generates a random float between min and max
func randFloat(min, max float64) float64 {
	return min + rand.Float64()*(max-min)
}
