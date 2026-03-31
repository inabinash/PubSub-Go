package app

import (
	"context"
	"database/sql"

	"github.com/avi/pubsub/pkg/events"
	"github.com/avi/pubsub/pkg/matcher"
	"github.com/avi/pubsub/pkg/models"
	"github.com/avi/pubsub/pkg/profile"
	"github.com/avi/pubsub/pkg/store"
)

type Engine struct {
	Profiles *profile.Service
	Ads      *store.SQLAdStore
	Matcher  *matcher.Service
}

// NewEngine wires all core services around a shared SQL database connection.
// It constructs profile storage, ad storage, and matcher dependencies once.
func NewEngine(db *sql.DB) *Engine {
	adStore := store.NewSQLAdStore(db)
	return &Engine{
		Profiles: profile.NewService(db, nil),
		Ads:      adStore,
		Matcher:  matcher.NewService(adStore),
	}
}

// SubscribeUserTopic records an explicit topic subscription for a user.
func (e *Engine) SubscribeUserTopic(ctx context.Context, userID, topic string) error {
	return e.Profiles.Subscribe(ctx, userID, topic)
}

// PublishAd upserts ad metadata and topic mappings into SQL.
func (e *Engine) PublishAd(ctx context.Context, ad models.Ad) error {
	return e.Ads.Upsert(ctx, ad)
}

// ProcessBehavior sends one behavior event through profile scoring and persistence.
func (e *Engine) ProcessBehavior(ctx context.Context, event events.BehaviorEvent) error {
	return e.Profiles.ProcessEvent(ctx, event)
}

// FetchAds loads the user's current profile snapshot and returns ranked matches.
func (e *Engine) FetchAds(ctx context.Context, req events.DeliveryRequest) ([]matcher.ScoredAd, error) {
	profile, err := e.Profiles.Snapshot(ctx, req.UserID)
	if err != nil {
		return nil, err
	}
	return e.Matcher.Match(ctx, profile, req)
}
