package httpapi

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/avi/pubsub/pkg/app"
	"github.com/avi/pubsub/pkg/events"
	"github.com/avi/pubsub/pkg/models"
)

type Server struct {
	engine *app.Engine
	mux    *http.ServeMux
}

// NewServer builds the HTTP transport layer and registers all routes.
func NewServer(engine *app.Engine) *Server {
	s := &Server{
		engine: engine,
		mux:    http.NewServeMux(),
	}
	s.routes()
	return s
}

// Handler returns the configured multiplexer so callers can mount/start the server.
func (s *Server) Handler() http.Handler {
	return s.mux
}

// routes maps HTTP endpoints to handler methods.
func (s *Server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("POST /v1/ads", s.handlePublishAd)
	s.mux.HandleFunc("POST /v1/subscriptions", s.handleSubscribe)
	s.mux.HandleFunc("POST /v1/events", s.handleEvent)
	s.mux.HandleFunc("POST /v1/ads/fetch", s.handleFetchAds)
}

// handleHealthz is a minimal liveness endpoint for health checks.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handlePublishAd validates and normalizes incoming ad payloads, then persists them.
// It fills missing operational defaults (timestamps/status) before upserting.
func (s *Server) handlePublishAd(w http.ResponseWriter, r *http.Request) {
	var ad models.Ad
	if err := json.NewDecoder(r.Body).Decode(&ad); err != nil {
		writeError(w, http.StatusBadRequest, "invalid ad payload")
		return
	}
	if ad.ID == "" || ad.CampaignID == "" || ad.AdvertiserID == "" || len(ad.Topics) == 0 {
		writeError(w, http.StatusBadRequest, "id, campaign_id, advertiser_id and topics are required")
		return
	}
	now := time.Now().UTC()
	if ad.CreatedAt.IsZero() {
		ad.CreatedAt = now
	}
	if ad.UpdatedAt.IsZero() {
		ad.UpdatedAt = now
	}
	if ad.StartTime.IsZero() {
		ad.StartTime = now
	}
	if ad.EndTime.IsZero() {
		ad.EndTime = now.Add(24 * time.Hour)
	}
	if ad.Status == "" {
		ad.Status = models.AdStatusActive
	}

	if err := s.engine.PublishAd(r.Context(), ad); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"status": "stored", "ad_id": ad.ID})
}

type subscribeRequest struct {
	UserID string `json:"user_id"`
	Topic  string `json:"topic"`
}

// handleSubscribe stores a user's explicit topic subscription.
func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	var req subscribeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid subscription payload")
		return
	}
	if req.UserID == "" || req.Topic == "" {
		writeError(w, http.StatusBadRequest, "user_id and topic are required")
		return
	}

	if err := s.engine.SubscribeUserTopic(r.Context(), req.UserID, req.Topic); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{"status": "subscribed"})
}

// handleEvent ingests one behavior event, updates profile state, and returns the snapshot.
// This gives immediate visibility into new derived topics after each signal.
func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	var event events.BehaviorEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		writeError(w, http.StatusBadRequest, "invalid event payload")
		return
	}
	if event.EventID == "" || event.UserID == "" || event.Topic == "" || event.Type == "" {
		writeError(w, http.StatusBadRequest, "event_id, user_id, type, and topic are required")
		return
	}
	if event.OccurredAt.IsZero() {
		event.OccurredAt = time.Now().UTC()
	}
	if event.Metadata == nil {
		event.Metadata = map[string]string{}
	}

	if err := s.engine.ProcessBehavior(r.Context(), event); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	profile, err := s.engine.Profiles.Snapshot(r.Context(), event.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, profile)
}

// handleFetchAds resolves a user profile and returns ranked ad matches for the request.
// Response includes both the matched ads and profile state used for matching.
func (s *Server) handleFetchAds(w http.ResponseWriter, r *http.Request) {
	var req events.DeliveryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid fetch payload")
		return
	}
	if req.UserID == "" {
		writeError(w, http.StatusBadRequest, "user_id is required")
		return
	}
	if req.RequestAt.IsZero() {
		req.RequestAt = time.Now().UTC()
	}

	matches, err := s.engine.FetchAds(r.Context(), req)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	profile, err := s.engine.Profiles.Snapshot(r.Context(), req.UserID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"user_id":    req.UserID,
		"request_at": req.RequestAt,
		"profile":    profile,
		"results":    matches,
	})
}

// writeJSON writes a JSON response with status code and content-type headers.
func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// writeError standardizes JSON error responses.
func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
