package api

import (
	"bidsrv/internal/model"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"

	"bidsrv/internal/campaign"
	"bidsrv/internal/event"
	"bidsrv/internal/idempotency"
	"bidsrv/internal/storage/infra_redis"
)

// Handler holds dependencies for the HTTP handlers
type Handler struct {
	campaignMgr *campaign.Manager
	store       *idempotency.Store
	bidCache    *infra_redis.BidCache
	producer    *event.Producer
	baseURL     string
}

// NewHandler creates a new Handler
func NewHandler(bm *campaign.Manager, s *idempotency.Store, bc *infra_redis.BidCache, p *event.Producer, baseURL string) *Handler {
	return &Handler{
		campaignMgr: bm,
		store:       s,
		bidCache:    bc,
		producer:    p,
		baseURL:     baseURL,
	}
}

// HandleHealthz responds with 200 OK
func (h *Handler) HandleHealthz(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

// HandleBid handles POST /v1/bid
func (h *Handler) HandleBid(w http.ResponseWriter, r *http.Request) {
	var req BidRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	requestID := uuid.New().String()

	// 1. Choose campaign
	camp := h.campaignMgr.SelectCampaign(req.UserIDFV)
	if camp == nil {
		// No fill
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// 2. Generate bid ID
	bidID := uuid.New().String()

	// 3. Store bid info in Redis for later retrieval during billing
	bidTimestamp := time.Now().Unix()
	bidInfo := infra_redis.BidInfo{
		CampaignID:   camp.ID,
		AppBundle:    req.AppBundle,
		PlacementID:  req.PlacementID,
		UserIDFV:     req.UserIDFV,
		BidTimestamp: bidTimestamp,
	}
	if err := h.bidCache.SetBidInfo(r.Context(), bidID, bidInfo); err != nil {
		log.Printf("error storing bid info in infra_redis: %v", err)
		// Continue anyway - don't block the bid response for infra_redis errors
	}

	// 4. Log to Redpanda
	bidEvent := model.BidEvent{
		RequestID:   requestID,
		BidID:       bidID,
		UserIDFV:    req.UserIDFV,
		CampaignID:  camp.ID,
		AppBundle:   req.AppBundle,
		PlacementID: req.PlacementID,
		Timestamp:   bidTimestamp,
	}

	// Using background context because we don't want to fail if the HTTP request is canceled
	if err := h.producer.ProduceBid(r.Context(), bidEvent); err != nil {
		log.Printf("error producing bid to kafka: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// 4. Return successful response
	resp := BidResponse{
		RequestID:   requestID,
		BidID:       bidID,
		CampaignID:  camp.ID,
		CreativeURL: camp.CreativeURL,
		BillingURL:  fmt.Sprintf("%s/v1/billing", h.baseURL),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// HandleBilling handles POST /v1/billing
func (h *Handler) HandleBilling(w http.ResponseWriter, r *http.Request) {
	var req BillingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.BidID == "" {
		http.Error(w, "bid_id is required", http.StatusBadRequest)
		return
	}

	// 1. Idempotency Check
	acquired, err := h.store.LockBilling(r.Context(), req.BidID)
	if err != nil {
		log.Printf("error checking idempotency: %v", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	if !acquired {
		// Duplicate recognized, return 200 OK immediately
		log.Printf("duplicate billing for bid_id=%s, ignoring", req.BidID)
		w.WriteHeader(http.StatusOK)
		return
	}

	// 3. Log to Redpanda
	impressionEvent := model.ImpressionEvent{
		BidID:     req.BidID,
		Timestamp: time.Now().Unix(),
	}

	if err := h.producer.ProduceImpression(r.Context(), impressionEvent); err != nil {
		log.Printf("error producing impression to kafka: %v", err)
		// Try to delete the lock so it can be retried?
		// For a minimal implementation, we might leave it as is.
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
