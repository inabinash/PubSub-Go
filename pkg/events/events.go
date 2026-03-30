package events

import "time"

type BehaviorType string

const (
	BehaviorWatch      BehaviorType = "watch"
	BehaviorClick      BehaviorType = "click"
	BehaviorImpression BehaviorType = "impression"
	BehaviorConversion BehaviorType = "conversion"
)

type BehaviorEvent struct {
	EventID string `json:"event_id"`
	UserID  string `json:"user_id"`

	Type  BehaviorType `json:"type"`
	Topic string       `json:"topic"`

	OccurredAt time.Time         `json:"occurred_at"`
	Metadata   map[string]string `json:"metadata"`
}

type DeliveryRequest struct {
	UserID string `json:"user_id"`

	ContextTopics []string  `json:"context_topics"`
	Limit         int       `json:"limit"`
	RequestAt     time.Time `json:"request_at"`
}
