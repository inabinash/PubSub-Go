package main

import (
	"log"
	"net/http"
	"os"

	"github.com/avi/pubsub/pkg/app"
	"github.com/avi/pubsub/pkg/db"
	"github.com/avi/pubsub/pkg/httpapi"
)

func main() {
	addr := env("ADDR", ":8080")
	dbPath := env("ADS_DB_PATH", "file:ads.db?_foreign_keys=on")

	sqlDB, err := db.OpenSQLite(dbPath)
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer sqlDB.Close()

	engine := app.NewEngine(sqlDB)
	server := httpapi.NewServer(engine)

	log.Printf("HTTP server listening on %s", addr)
	if err := http.ListenAndServe(addr, server.Handler()); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func env(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
