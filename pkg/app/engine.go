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

func NewEngine(db *sql.DB) *Engine {
	adStore := store.NewSQLAdStore(db)
	return &Engine{
		Profiles: profile.NewService(db, nil),
		Ads:      adStore,
		Matcher:  matcher.NewService(adStore),
	}
}

func (e *Engine) SubscribeUserTopic(ctx context.Context, userID, topic string) error {
	return e.Profiles.Subscribe(ctx, userID, topic)
}

func (e *Engine) PublishAd(ctx context.Context, ad models.Ad) error {
	return e.Ads.Upsert(ctx, ad)
}

func (e *Engine) ProcessBehavior(ctx context.Context, event events.BehaviorEvent) error {
	return e.Profiles.ProcessEvent(ctx, event)
}

func (e *Engine) FetchAds(ctx context.Context, req events.DeliveryRequest) ([]matcher.ScoredAd, error) {
	profile, err := e.Profiles.Snapshot(ctx, req.UserID)
	if err != nil {
		return nil, err
	}
	return e.Matcher.Match(ctx, profile, req)
}
