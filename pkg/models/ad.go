package models

import "time"

type AdStatus string

const (
	AdStatusDraft   AdStatus = "draft"
	AdStatusActive  AdStatus = "active"
	AdStatusPaused  AdStatus = "paused"
	AdStatusExpired AdStatus = "expired"
)

type Ad struct {
	ID           string `json:"id"`
	CampaignID   string `json:"campaign_id"`
	AdvertiserID string `json:"advertiser_id"`

	Topics   []string `json:"topics"`
	Keywords []string `json:"keywords"`

	TargetInterests   []string `json:"target_interests"`
	ExcludedInterests []string `json:"excluded_interests"`

	Bid         float64 `json:"bid"`
	BudgetDaily float64 `json:"budget_daily"`
	BudgetTotal float64 `json:"budget_total"`
	Priority    int     `json:"priority"`

	StartTime    time.Time `json:"start_time"`
	EndTime      time.Time `json:"end_time"`
	FrequencyCap int       `json:"frequency_cap"`

	CreativeType string `json:"creative_type"`
	AssetURL     string `json:"asset_url"`
	CTA          string `json:"cta"`
	LandingURL   string `json:"landing_url"`

	Status    AdStatus  `json:"status"`
	Version   int       `json:"version"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

func (a Ad) IsActive(now time.Time) bool {
	if a.Status != AdStatusActive {
		return false
	}
	if !a.StartTime.IsZero() && now.Before(a.StartTime) {
		return false
	}
	if !a.EndTime.IsZero() && now.After(a.EndTime) {
		return false
	}
	return true
}
