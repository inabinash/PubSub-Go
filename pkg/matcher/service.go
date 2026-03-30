package matcher

import (
	"context"
	"sort"
	"time"

	"github.com/avi/pubsub/pkg/events"
	"github.com/avi/pubsub/pkg/models"
)

type AdReader interface {
	GetByTopics(ctx context.Context, topics []string) ([]models.Ad, error)
}

type Service struct {
	store AdReader
}

type ScoredAd struct {
	Ad    models.Ad `json:"ad"`
	Score float64   `json:"score"`
}

func NewService(store AdReader) *Service {
	return &Service{store: store}
}

func (m *Service) Match(ctx context.Context, profile *models.UserProfile, req events.DeliveryRequest) ([]ScoredAd, error) {
	if req.Limit <= 0 {
		req.Limit = 5
	}
	if req.RequestAt.IsZero() {
		req.RequestAt = time.Now()
	}

	topics := profile.AllTopics(req.ContextTopics)
	candidates, err := m.store.GetByTopics(ctx, topics)
	if err != nil {
		return nil, err
	}

	scored := make([]ScoredAd, 0, len(candidates))
	for _, ad := range candidates {
		if !ad.IsActive(req.RequestAt) {
			continue
		}
		if !isEligibleForProfile(ad, profile) {
			continue
		}
		scored = append(scored, ScoredAd{
			Ad:    ad,
			Score: score(ad, profile, req.ContextTopics),
		})
	}

	sort.Slice(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			return scored[i].Ad.Priority > scored[j].Ad.Priority
		}
		return scored[i].Score > scored[j].Score
	})

	if len(scored) > req.Limit {
		scored = scored[:req.Limit]
	}

	return scored, nil
}

func isEligibleForProfile(ad models.Ad, profile *models.UserProfile) bool {
	if intersects(ad.ExcludedInterests, profile.DerivedTopics) {
		return false
	}
	if len(ad.TargetInterests) == 0 {
		return true
	}
	if intersects(ad.TargetInterests, profile.ExplicitTopics) {
		return true
	}
	if intersects(ad.TargetInterests, profile.DerivedTopics) {
		return true
	}
	for _, target := range ad.TargetInterests {
		if profile.Interest(target) > 0 {
			return true
		}
	}
	return false
}

func score(ad models.Ad, profile *models.UserProfile, contextTopics []string) float64 {
	value := ad.Bid + float64(ad.Priority)

	allTopics := profile.AllTopics(contextTopics)
	allSet := make(map[string]struct{}, len(allTopics))
	for _, topic := range allTopics {
		allSet[topic] = struct{}{}
	}

	for _, topic := range ad.Topics {
		if _, ok := allSet[topic]; ok {
			value += 1.25
		}
	}

	for _, target := range ad.TargetInterests {
		value += profile.Interest(target) * 0.5
	}

	return value
}

func intersects(targets []string, set map[string]struct{}) bool {
	for _, target := range targets {
		if _, ok := set[target]; ok {
			return true
		}
	}
	return false
}
