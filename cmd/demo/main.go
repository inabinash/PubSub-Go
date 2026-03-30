package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/avi/pubsub/pkg/app"
	"github.com/avi/pubsub/pkg/db"
	"github.com/avi/pubsub/pkg/events"
	"github.com/avi/pubsub/pkg/models"
)

func main() {
	ctx := context.Background()
	sqlDB, err := db.OpenSQLite("file:demo.db?_foreign_keys=on")
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer sqlDB.Close()

	engine := app.NewEngine(sqlDB)
	userID := "user-42"

	if err := engine.SubscribeUserTopic(ctx, userID, "sports"); err != nil {
		log.Fatalf("subscribe user: %v", err)
	}

	now := time.Now().UTC()
	if err := engine.PublishAd(ctx, models.Ad{
		ID:              "ad-sports-shoes-1",
		CampaignID:      "cmp-sports-q2",
		AdvertiserID:    "brand-nike",
		Topics:          []string{"sports", "sports_shoes"},
		TargetInterests: []string{"football", "football_fan"},
		Bid:             4.8,
		Priority:        3,
		Status:          models.AdStatusActive,
		StartTime:       now.Add(-1 * time.Hour),
		EndTime:         now.Add(30 * 24 * time.Hour),
		CreatedAt:       now,
		UpdatedAt:       now,
	}); err != nil {
		log.Fatalf("publish ad: %v", err)
	}

	for i := 0; i < 4; i++ {
		if err := engine.ProcessBehavior(ctx, events.BehaviorEvent{
			EventID:    fmt.Sprintf("evt-watch-football-%d", i),
			UserID:     userID,
			Type:       events.BehaviorWatch,
			Topic:      "football",
			OccurredAt: time.Now().UTC(),
		}); err != nil {
			log.Fatalf("process event: %v", err)
		}
	}

	matches, err := engine.FetchAds(ctx, events.DeliveryRequest{
		UserID:        userID,
		ContextTopics: []string{"live_match"},
		Limit:         3,
		RequestAt:     time.Now().UTC(),
	})
	if err != nil {
		log.Fatalf("fetch ads: %v", err)
	}

	fmt.Println("Matched ads (ranked):")
	for i, ad := range matches {
		fmt.Printf("%d) ad=%s score=%.2f topics=%v targets=%v\n",
			i+1, ad.Ad.ID, ad.Score, ad.Ad.Topics, ad.Ad.TargetInterests)
	}
}
