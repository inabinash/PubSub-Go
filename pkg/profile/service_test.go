package profile

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/avi/pubsub/pkg/db"
	"github.com/avi/pubsub/pkg/events"
)

func TestDerivesFootballTopics(t *testing.T) {
	sqlDB, err := db.OpenSQLite("file::memory:?cache=shared")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer sqlDB.Close()

	svc := NewService(sqlDB, nil)
	userID := "u1"
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := svc.ProcessEvent(ctx, events.BehaviorEvent{
			EventID:    fmt.Sprintf("evt-%d", i),
			UserID:     userID,
			Type:       events.BehaviorWatch,
			Topic:      "football",
			OccurredAt: time.Now(),
		}); err != nil {
			t.Fatalf("process event: %v", err)
		}
	}

	profile, err := svc.Snapshot(ctx, userID)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if _, ok := profile.DerivedTopics["sports_shoes"]; !ok {
		t.Fatalf("expected sports_shoes in derived topics: %+v", profile.DerivedTopics)
	}
	if profile.Interest("football") < 3 {
		t.Fatalf("expected football score >= 3, got %v", profile.Interest("football"))
	}
}
