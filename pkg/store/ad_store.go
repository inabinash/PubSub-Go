package store

import (
	"sync"

	"github.com/avi/pubsub/pkg/models"
)

type InMemoryAdStore struct {
	mu sync.RWMutex

	ads        map[string]models.Ad
	topicIndex map[string]map[string]struct{}
}

func NewInMemoryAdStore() *InMemoryAdStore {
	return &InMemoryAdStore{
		ads:        make(map[string]models.Ad),
		topicIndex: make(map[string]map[string]struct{}),
	}
}

func (s *InMemoryAdStore) Upsert(ad models.Ad) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if prev, exists := s.ads[ad.ID]; exists {
		for _, topic := range prev.Topics {
			if ids, ok := s.topicIndex[topic]; ok {
				delete(ids, prev.ID)
				if len(ids) == 0 {
					delete(s.topicIndex, topic)
				}
			}
		}
	}

	s.ads[ad.ID] = ad
	for _, topic := range ad.Topics {
		if _, ok := s.topicIndex[topic]; !ok {
			s.topicIndex[topic] = make(map[string]struct{})
		}
		s.topicIndex[topic][ad.ID] = struct{}{}
	}
}

func (s *InMemoryAdStore) GetByTopics(topics []string) []models.Ad {
	s.mu.RLock()
	defer s.mu.RUnlock()

	seen := make(map[string]struct{})
	out := make([]models.Ad, 0)

	for _, topic := range topics {
		for adID := range s.topicIndex[topic] {
			if _, ok := seen[adID]; ok {
				continue
			}
			seen[adID] = struct{}{}
			out = append(out, s.ads[adID])
		}
	}

	return out
}
