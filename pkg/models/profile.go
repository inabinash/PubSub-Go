package models

import (
	"sort"
	"time"
)

type UserProfile struct {
	UserID string `json:"user_id"`

	ExplicitTopics map[string]struct{} `json:"explicit_topics"`
	DerivedTopics  map[string]struct{} `json:"derived_topics"`
	InterestScores map[string]float64  `json:"interest_scores"`

	LastUpdated time.Time `json:"last_updated"`
}

func NewUserProfile(userID string) *UserProfile {
	return &UserProfile{
		UserID:         userID,
		ExplicitTopics: make(map[string]struct{}),
		DerivedTopics:  make(map[string]struct{}),
		InterestScores: make(map[string]float64),
		LastUpdated:    time.Now(),
	}
}

func (p *UserProfile) Clone() *UserProfile {
	dup := &UserProfile{
		UserID:         p.UserID,
		ExplicitTopics: make(map[string]struct{}, len(p.ExplicitTopics)),
		DerivedTopics:  make(map[string]struct{}, len(p.DerivedTopics)),
		InterestScores: make(map[string]float64, len(p.InterestScores)),
		LastUpdated:    p.LastUpdated,
	}
	for topic := range p.ExplicitTopics {
		dup.ExplicitTopics[topic] = struct{}{}
	}
	for topic := range p.DerivedTopics {
		dup.DerivedTopics[topic] = struct{}{}
	}
	for topic, score := range p.InterestScores {
		dup.InterestScores[topic] = score
	}
	return dup
}

func (p *UserProfile) SubscribeExplicit(topic string) {
	p.ExplicitTopics[topic] = struct{}{}
	p.LastUpdated = time.Now()
}

func (p *UserProfile) SetDerivedTopics(topics []string) {
	p.DerivedTopics = make(map[string]struct{}, len(topics))
	for _, topic := range topics {
		p.DerivedTopics[topic] = struct{}{}
	}
	p.LastUpdated = time.Now()
}

func (p *UserProfile) Interest(topic string) float64 {
	return p.InterestScores[topic]
}

func (p *UserProfile) SetInterest(topic string, score float64) {
	p.InterestScores[topic] = score
	p.LastUpdated = time.Now()
}

func (p *UserProfile) AllTopics(contextTopics []string) []string {
	set := make(map[string]struct{}, len(p.ExplicitTopics)+len(p.DerivedTopics)+len(contextTopics))
	for topic := range p.ExplicitTopics {
		set[topic] = struct{}{}
	}
	for topic := range p.DerivedTopics {
		set[topic] = struct{}{}
	}
	for _, topic := range contextTopics {
		set[topic] = struct{}{}
	}

	out := make([]string, 0, len(set))
	for topic := range set {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}
