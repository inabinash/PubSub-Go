package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/avi/pubsub/pkg/app"
	"github.com/avi/pubsub/pkg/db"
)

func TestHTTPFlow_SubscribeEventPublishFetch(t *testing.T) {
	sqlDB, err := db.OpenSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqlDB.Close()

	engine := app.NewEngine(sqlDB)
	server := NewServer(engine)
	handler := server.Handler()

	doJSON := func(method, path string, payload any) *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(payload)
		req := httptest.NewRequest(method, path, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	rec := doJSON(http.MethodPost, "/v1/subscriptions", map[string]any{
		"user_id": "u1",
		"topic":   "sports",
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("subscribe status=%d body=%s", rec.Code, rec.Body.String())
	}

	for i := 1; i <= 3; i++ {
		rec = doJSON(http.MethodPost, "/v1/events", map[string]any{
			"event_id":    fmt.Sprintf("ev-%d", i),
			"user_id":     "u1",
			"type":        "watch",
			"topic":       "football",
			"occurred_at": time.Now().UTC().Format(time.RFC3339Nano),
		})
		if rec.Code != http.StatusOK {
			t.Fatalf("event status=%d body=%s", rec.Code, rec.Body.String())
		}
	}

	rec = doJSON(http.MethodPost, "/v1/ads", map[string]any{
		"id":               "ad1",
		"campaign_id":      "cmp1",
		"advertiser_id":    "nike",
		"topics":           []string{"sports", "sports_shoes"},
		"target_interests": []string{"football", "football_fan"},
		"bid":              4.2,
		"priority":         2,
		"status":           "active",
		"start_time":       time.Now().UTC().Add(-time.Hour).Format(time.RFC3339Nano),
		"end_time":         time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano),
	})
	if rec.Code != http.StatusCreated {
		t.Fatalf("publish status=%d body=%s", rec.Code, rec.Body.String())
	}

	rec = doJSON(http.MethodPost, "/v1/ads/fetch", map[string]any{
		"user_id":        "u1",
		"context_topics": []string{"live_match"},
		"limit":          3,
	})
	if rec.Code != http.StatusOK {
		t.Fatalf("fetch status=%d body=%s", rec.Code, rec.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode fetch response: %v", err)
	}

	results, ok := resp["results"].([]any)
	if !ok {
		t.Fatalf("expected results array in response: %v", resp)
	}
	if len(results) == 0 {
		t.Fatalf("expected at least 1 matched ad: %v", resp)
	}

	profile, err := engine.Profiles.Snapshot(context.Background(), "u1")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if _, exists := profile.DerivedTopics["sports_shoes"]; !exists {
		t.Fatalf("expected sports_shoes derived topic, got: %+v", profile.DerivedTopics)
	}
}
