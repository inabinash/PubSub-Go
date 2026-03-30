package pubsub

import (
	"sync"

	"github.com/avi/pubsub/pkg/models"
)

type InMemoryBroker struct {
	mu          sync.RWMutex
	subscribers map[string]map[string]chan models.Ad
}

func NewInMemoryBroker() *InMemoryBroker {
	return &InMemoryBroker{
		subscribers: make(map[string]map[string]chan models.Ad),
	}
}

func (b *InMemoryBroker) Subscribe(topic, subscriberID string, buffer int) (<-chan models.Ad, func()) {
	if buffer <= 0 {
		buffer = 16
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, ok := b.subscribers[topic]; !ok {
		b.subscribers[topic] = make(map[string]chan models.Ad)
	}

	ch := make(chan models.Ad, buffer)
	b.subscribers[topic][subscriberID] = ch

	unsubscribe := func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		subs, ok := b.subscribers[topic]
		if !ok {
			return
		}
		if c, exists := subs[subscriberID]; exists {
			close(c)
			delete(subs, subscriberID)
		}
		if len(subs) == 0 {
			delete(b.subscribers, topic)
		}
	}

	return ch, unsubscribe
}

func (b *InMemoryBroker) Publish(topic string, ad models.Ad) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, subscriber := range b.subscribers[topic] {
		select {
		case subscriber <- ad:
		default:
			// Slow subscribers are skipped to protect publisher latency.
		}
	}
}
