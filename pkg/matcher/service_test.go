package matcher

import (
	"context"
	"testing"
	"time"

	"github.com/avi/pubsub/pkg/events"
	"github.com/avi/pubsub/pkg/models"
)

type mockStore struct {
	ads []models.Ad
}

func (m *mockStore) GetByTopics(_ context.Context, _ []string) ([]models.Ad, error) {
	return m.ads, nil
}

func TestMatchFiltersAndRanks(t *testing.T) {
	now := time.Now()
	m := NewService(&mockStore{
		ads: []models.Ad{
			{
				ID:              "ad1",
				Topics:          []string{"sports"},
				TargetInterests: []string{"football"},
				Bid:             5,
				Priority:        2,
				Status:          models.AdStatusActive,
				StartTime:       now.Add(-time.Hour),
				EndTime:         now.Add(time.Hour),
			},
			{
				ID:              "ad2",
				Topics:          []string{"sports"},
				TargetInterests: []string{"gaming"},
				Bid:             6,
				Priority:        1,
				Status:          models.AdStatusActive,
				StartTime:       now.Add(-time.Hour),
				EndTime:         now.Add(time.Hour),
			},
		},
	})

	p := models.NewUserProfile("u1")
	p.SubscribeExplicit("sports")
	p.SetInterest("football", 4)
	p.SetDerivedTopics([]string{"football_fan", "sports"})

	out, err := m.Match(context.Background(), p, events.DeliveryRequest{
		UserID:    "u1",
		Limit:     3,
		RequestAt: now,
	})
	if err != nil {
		t.Fatalf("unexpected match error: %v", err)
	}

	if len(out) != 1 {
		t.Fatalf("expected 1 matched ad, got %d", len(out))
	}
	if out[0].Ad.ID != "ad1" {
		t.Fatalf("expected ad1 as match, got %s", out[0].Ad.ID)
	}
}
