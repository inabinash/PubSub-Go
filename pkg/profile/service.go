package profile

import (
	"context"
	"database/sql"
	"encoding/json"
	"sort"
	"time"

	"github.com/avi/pubsub/pkg/events"
	"github.com/avi/pubsub/pkg/models"
)

type Scorer struct {
	weights map[events.BehaviorType]float64
}

func NewDefaultScorer() *Scorer {
	return &Scorer{
		weights: map[events.BehaviorType]float64{
			events.BehaviorWatch:      1.0,
			events.BehaviorClick:      2.0,
			events.BehaviorImpression: 0.25,
			events.BehaviorConversion: 3.0,
		},
	}
}

func (s *Scorer) Weight(t events.BehaviorType) float64 {
	weight, ok := s.weights[t]
	if !ok {
		return 0.5
	}
	return weight
}

type Service struct {
	db     *sql.DB
	scorer *Scorer
}

func NewService(db *sql.DB, scorer *Scorer) *Service {
	if scorer == nil {
		scorer = NewDefaultScorer()
	}
	return &Service{db: db, scorer: scorer}
}

func (s *Service) Subscribe(ctx context.Context, userID, topic string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO profiles (user_id, last_updated)
		VALUES (?, ?)
		ON CONFLICT(user_id) DO UPDATE SET last_updated = excluded.last_updated
	`, userID, now)
	if err != nil {
		return err
	}

	_, err = s.db.ExecContext(ctx, `
		INSERT INTO subscriptions (user_id, topic, source, created_at)
		VALUES (?, ?, 'explicit', ?)
		ON CONFLICT(user_id, topic, source) DO NOTHING
	`, userID, topic, now)
	return err
}

func (s *Service) ProcessEvent(ctx context.Context, event events.BehaviorEvent) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	occurred := event.OccurredAt.UTC().Format(time.RFC3339Nano)
	metaJSON, _ := json.Marshal(event.Metadata)

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO profiles (user_id, last_updated)
		VALUES (?, ?)
		ON CONFLICT(user_id) DO UPDATE SET last_updated = excluded.last_updated
	`, event.UserID, now); err != nil {
		return err
	}

	res, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO behavior_events
		(event_id, user_id, event_type, topic, occurred_at, metadata_json, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, event.EventID, event.UserID, string(event.Type), event.Topic, occurred, string(metaJSON), now)
	if err != nil {
		return err
	}

	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return tx.Commit()
	}

	delta := s.scorer.Weight(event.Type)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO interest_scores (user_id, topic, score, last_updated)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(user_id, topic) DO UPDATE SET
			score = interest_scores.score + excluded.score,
			last_updated = excluded.last_updated
	`, event.UserID, event.Topic, delta, now); err != nil {
		return err
	}

	scoreRows, err := tx.QueryContext(ctx, `
		SELECT topic, score FROM interest_scores WHERE user_id = ?
	`, event.UserID)
	if err != nil {
		return err
	}
	defer scoreRows.Close()

	scores := make(map[string]float64)
	for scoreRows.Next() {
		var topic string
		var score float64
		if err := scoreRows.Scan(&topic, &score); err != nil {
			return err
		}
		scores[topic] = score
	}
	if err := scoreRows.Err(); err != nil {
		return err
	}

	derived := deriveTopicsFromScores(scores)
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM subscriptions WHERE user_id = ? AND source = 'derived'
	`, event.UserID); err != nil {
		return err
	}

	for _, topic := range derived {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO subscriptions (user_id, topic, source, created_at)
			VALUES (?, ?, 'derived', ?)
		`, event.UserID, topic, now); err != nil {
			return err
		}
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE profiles SET last_updated = ? WHERE user_id = ?
	`, now, event.UserID); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Service) Snapshot(ctx context.Context, userID string) (*models.UserProfile, error) {
	p := models.NewUserProfile(userID)

	var lastUpdated string
	err := s.db.QueryRowContext(ctx, `
		SELECT last_updated FROM profiles WHERE user_id = ?
	`, userID).Scan(&lastUpdated)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if err == nil {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, lastUpdated); parseErr == nil {
			p.LastUpdated = parsed
		}
	}

	subRows, err := s.db.QueryContext(ctx, `
		SELECT topic, source FROM subscriptions WHERE user_id = ?
	`, userID)
	if err != nil {
		return nil, err
	}
	defer subRows.Close()

	for subRows.Next() {
		var topic string
		var source string
		if err := subRows.Scan(&topic, &source); err != nil {
			return nil, err
		}
		if source == "explicit" {
			p.ExplicitTopics[topic] = struct{}{}
		}
		if source == "derived" {
			p.DerivedTopics[topic] = struct{}{}
		}
	}
	if err := subRows.Err(); err != nil {
		return nil, err
	}

	scoreRows, err := s.db.QueryContext(ctx, `
		SELECT topic, score FROM interest_scores WHERE user_id = ?
	`, userID)
	if err != nil {
		return nil, err
	}
	defer scoreRows.Close()

	for scoreRows.Next() {
		var topic string
		var score float64
		if err := scoreRows.Scan(&topic, &score); err != nil {
			return nil, err
		}
		p.InterestScores[topic] = score
	}
	if err := scoreRows.Err(); err != nil {
		return nil, err
	}

	return p, nil
}

func deriveTopicsFromScores(scores map[string]float64) []string {
	set := make(map[string]struct{})

	for topic, score := range scores {
		if score >= 2.0 {
			set[topic] = struct{}{}
		}
	}

	if scores["football"] >= 3.0 {
		set["football_fan"] = struct{}{}
		set["sports"] = struct{}{}
		set["sports_shoes"] = struct{}{}
	}

	if scores["cricket"] >= 3.0 {
		set["cricket_fan"] = struct{}{}
		set["sports"] = struct{}{}
	}

	out := make([]string, 0, len(set))
	for topic := range set {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}
