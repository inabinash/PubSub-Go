package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/avi/pubsub/pkg/models"
)

type SQLAdStore struct {
	db *sql.DB
}

func NewSQLAdStore(db *sql.DB) *SQLAdStore {
	return &SQLAdStore{db: db}
}

func (s *SQLAdStore) Upsert(ctx context.Context, ad models.Ad) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	keywordsJSON, _ := json.Marshal(ad.Keywords)
	targetJSON, _ := json.Marshal(ad.TargetInterests)
	excludedJSON, _ := json.Marshal(ad.ExcludedInterests)

	start := ad.StartTime.UTC().Format(time.RFC3339Nano)
	end := ad.EndTime.UTC().Format(time.RFC3339Nano)
	created := ad.CreatedAt.UTC().Format(time.RFC3339Nano)
	updated := ad.UpdatedAt.UTC().Format(time.RFC3339Nano)

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO ads (
			id, campaign_id, advertiser_id, keywords_json, target_interests_json, excluded_interests_json,
			bid, budget_daily, budget_total, priority, start_time, end_time, frequency_cap,
			creative_type, asset_url, cta, landing_url, status, version, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			campaign_id = excluded.campaign_id,
			advertiser_id = excluded.advertiser_id,
			keywords_json = excluded.keywords_json,
			target_interests_json = excluded.target_interests_json,
			excluded_interests_json = excluded.excluded_interests_json,
			bid = excluded.bid,
			budget_daily = excluded.budget_daily,
			budget_total = excluded.budget_total,
			priority = excluded.priority,
			start_time = excluded.start_time,
			end_time = excluded.end_time,
			frequency_cap = excluded.frequency_cap,
			creative_type = excluded.creative_type,
			asset_url = excluded.asset_url,
			cta = excluded.cta,
			landing_url = excluded.landing_url,
			status = excluded.status,
			version = excluded.version,
			updated_at = excluded.updated_at
	`, ad.ID, ad.CampaignID, ad.AdvertiserID, string(keywordsJSON), string(targetJSON), string(excludedJSON),
		ad.Bid, ad.BudgetDaily, ad.BudgetTotal, ad.Priority, start, end, ad.FrequencyCap,
		ad.CreativeType, ad.AssetURL, ad.CTA, ad.LandingURL, string(ad.Status), ad.Version, created, updated); err != nil {
		return err
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM ad_topics WHERE ad_id = ?`, ad.ID); err != nil {
		return err
	}

	for _, topic := range ad.Topics {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO ad_topics (ad_id, topic) VALUES (?, ?)
		`, ad.ID, topic); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (s *SQLAdStore) GetByTopics(ctx context.Context, topics []string) ([]models.Ad, error) {
	if len(topics) == 0 {
		return nil, nil
	}

	placeholders := make([]string, len(topics))
	args := make([]any, 0, len(topics))
	for i, topic := range topics {
		placeholders[i] = "?"
		args = append(args, topic)
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT
			a.id, a.campaign_id, a.advertiser_id, a.keywords_json, a.target_interests_json,
			a.excluded_interests_json, a.bid, a.budget_daily, a.budget_total, a.priority,
			a.start_time, a.end_time, a.frequency_cap, a.creative_type, a.asset_url,
			a.cta, a.landing_url, a.status, a.version, a.created_at, a.updated_at
		FROM ads a
		INNER JOIN ad_topics t ON a.id = t.ad_id
		WHERE t.topic IN (%s)
	`, strings.Join(placeholders, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]models.Ad, 0)
	for rows.Next() {
		var ad models.Ad
		var keywordsJSON string
		var targetJSON string
		var excludedJSON string
		var status string
		var start string
		var end string
		var created string
		var updated string

		if err := rows.Scan(
			&ad.ID, &ad.CampaignID, &ad.AdvertiserID, &keywordsJSON, &targetJSON, &excludedJSON,
			&ad.Bid, &ad.BudgetDaily, &ad.BudgetTotal, &ad.Priority,
			&start, &end, &ad.FrequencyCap, &ad.CreativeType, &ad.AssetURL,
			&ad.CTA, &ad.LandingURL, &status, &ad.Version, &created, &updated,
		); err != nil {
			return nil, err
		}

		ad.Status = models.AdStatus(status)
		ad.StartTime, _ = time.Parse(time.RFC3339Nano, start)
		ad.EndTime, _ = time.Parse(time.RFC3339Nano, end)
		ad.CreatedAt, _ = time.Parse(time.RFC3339Nano, created)
		ad.UpdatedAt, _ = time.Parse(time.RFC3339Nano, updated)

		_ = json.Unmarshal([]byte(keywordsJSON), &ad.Keywords)
		_ = json.Unmarshal([]byte(targetJSON), &ad.TargetInterests)
		_ = json.Unmarshal([]byte(excludedJSON), &ad.ExcludedInterests)

		topicRows, err := s.db.QueryContext(ctx, `SELECT topic FROM ad_topics WHERE ad_id = ?`, ad.ID)
		if err != nil {
			return nil, err
		}
		ad.Topics = make([]string, 0)
		for topicRows.Next() {
			var topic string
			if err := topicRows.Scan(&topic); err != nil {
				topicRows.Close()
				return nil, err
			}
			ad.Topics = append(ad.Topics, topic)
		}
		topicRows.Close()

		out = append(out, ad)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
