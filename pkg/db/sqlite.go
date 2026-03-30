package db

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

func OpenSQLite(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = ON;`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := initSchema(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func initSchema(db *sql.DB) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS profiles (
			user_id TEXT PRIMARY KEY,
			last_updated TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS subscriptions (
			user_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			source TEXT NOT NULL,
			created_at TEXT NOT NULL,
			PRIMARY KEY (user_id, topic, source),
			FOREIGN KEY (user_id) REFERENCES profiles(user_id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS interest_scores (
			user_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			score REAL NOT NULL,
			last_updated TEXT NOT NULL,
			PRIMARY KEY (user_id, topic),
			FOREIGN KEY (user_id) REFERENCES profiles(user_id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS behavior_events (
			event_id TEXT PRIMARY KEY,
			user_id TEXT NOT NULL,
			event_type TEXT NOT NULL,
			topic TEXT NOT NULL,
			occurred_at TEXT NOT NULL,
			metadata_json TEXT NOT NULL,
			created_at TEXT NOT NULL,
			FOREIGN KEY (user_id) REFERENCES profiles(user_id) ON DELETE CASCADE
		);`,
		`CREATE TABLE IF NOT EXISTS ads (
			id TEXT PRIMARY KEY,
			campaign_id TEXT NOT NULL,
			advertiser_id TEXT NOT NULL,
			keywords_json TEXT NOT NULL,
			target_interests_json TEXT NOT NULL,
			excluded_interests_json TEXT NOT NULL,
			bid REAL NOT NULL,
			budget_daily REAL NOT NULL,
			budget_total REAL NOT NULL,
			priority INTEGER NOT NULL,
			start_time TEXT NOT NULL,
			end_time TEXT NOT NULL,
			frequency_cap INTEGER NOT NULL,
			creative_type TEXT NOT NULL,
			asset_url TEXT NOT NULL,
			cta TEXT NOT NULL,
			landing_url TEXT NOT NULL,
			status TEXT NOT NULL,
			version INTEGER NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS ad_topics (
			ad_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			PRIMARY KEY (ad_id, topic),
			FOREIGN KEY (ad_id) REFERENCES ads(id) ON DELETE CASCADE
		);`,
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("init schema failed: %w", err)
		}
	}
	return nil
}
