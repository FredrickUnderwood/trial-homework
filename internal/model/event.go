package model

type BidEvent struct {
	RequestID   string `json:"request_id"`
	BidID       string `json:"bid_id"`
	UserIDFV    string `json:"user_idfv"`
	CampaignID  string `json:"campaign_id"`
	AppBundle   string `json:"app_bundle"`
	PlacementID string `json:"placement_id"`
	Timestamp   int64  `json:"timestamp"`
}

type ImpressionEvent struct {
	BidID     string `json:"bid_id"`
	Timestamp int64  `json:"timestamp"`
}
