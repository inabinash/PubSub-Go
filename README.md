# Ads Pub-Sub API (Go + HTTP + SQLite)

This project now runs as an HTTP request/response service and persists data in SQLite instead of in-memory maps.

## Run

```bash
go run ./cmd/server
```

Optional environment variables:

- `ADDR` (default `:8080`)
- `ADS_DB_PATH` (default `file:ads.db?_foreign_keys=on`)

## API Endpoints

- `GET /healthz`
- `POST /v1/ads` (publish/store ad)
- `POST /v1/subscriptions` (explicit topic subscription)
- `POST /v1/events` (behavior event, updates profile + derived topics)
- `POST /v1/ads/fetch` (fetch ranked ads for a user)

## Example cURL

```bash
curl -X POST localhost:8080/v1/subscriptions \
  -H 'content-type: application/json' \
  -d '{"user_id":"u1","topic":"sports"}'

curl -X POST localhost:8080/v1/events \
  -H 'content-type: application/json' \
  -d '{"event_id":"e1","user_id":"u1","type":"watch","topic":"football","occurred_at":"2026-03-30T12:00:00Z"}'

curl -X POST localhost:8080/v1/ads/fetch \
  -H 'content-type: application/json' \
  -d '{"user_id":"u1","context_topics":["live_match"],"limit":3}'
```
