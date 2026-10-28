# HypeStream

A small live-streaming commerce platform: **Subscriptions, Gift Subs, Bits and Hype Train**, built in Go.
Viewers support a creator, every contribution is recorded once in a ledger, and every viewer watching the channel sees it live.

![tests](https://github.com/OWNER/REPO/actions/workflows/test.yml/badge.svg)

## What it does

| Feature | How it works |
|---|---|
| Bits | Viewers buy test Bits (100, 500, 1500, 5000, 10000) and cheer them in a channel. 1 Bit = 1 Hype Train point. |
| Subscriptions | Tier 1, 2 or 3. Renewing while active adds another 30 days on top. |
| Gift subs | Gift to named viewers, or to random viewers who are watching and not already subscribed. |
| Hype Train | Starts after 3 contributions in a 5 minute sliding window. Points fill levels, leftover points carry over, each contribution resets the 5 minute timer. |
| Live events | Server-Sent Events push cheers, subs, gifts and train updates to every viewer of the channel. |
| Leaderboard | Top supporters per channel by points. |
| Notifications | Gift receipts, gift received, and train started (logged now; email and push come in Phase 3). |

## Design decisions

- **Idempotency keys.** Every write needs an `Idempotency-Key` header. If a viewer double-taps or the network retries, the same key returns the first receipt (`"replayed": true`) and nothing is charged twice. Reusing a key with a different body returns `422`. A failed request does not store its key, so it can be retried.
- **Append-only ledger.** Money movements are never edited. A test checks that the ledger always sums to the wallet balance.
- **No overspending under load.** A test fires 100 parallel cheers at a wallet that can afford 15; exactly 15 succeed.
- **Slow viewers don't slow down others.** Each viewer connection has its own small buffer. If it is full, that viewer skips an event instead of blocking the broadcast.
- **Server-Sent Events instead of WebSockets.** Viewers only receive events; they send actions over normal HTTP. SSE is simpler, works through proxies, and needs no extra library.
- **Zero dependencies.** Only the Go standard library.
- **Testable time.** The Hype Train engine takes a clock function, so tests move time forward instead of sleeping.

## Project layout

```
backend/
  cmd/server/main.go           entry point, background timer that ends trains
  internal/hypetrain/          Hype Train engine (sliding window, levels, timer)
  internal/commerce/           Bits, cheers, subs, gifts, ledger, idempotency, leaderboard
  internal/events/             per-channel pub/sub for live events
  internal/api/                HTTP routes, SSE stream, error-to-status mapping
  migrations/001_init.sql      PostgreSQL schema (used from Phase 2)
  Dockerfile
.github/workflows/test.yml     gofmt, vet, race-detector tests, build
```

## API

| Method | Path | Body |
|---|---|---|
| GET | `/healthz` | |
| POST | `/v1/bits/purchases` | `{"user":"alice","amount":500}` |
| GET | `/v1/users/{user}/wallet` | |
| GET | `/v1/users/{user}/ledger` | |
| POST | `/v1/channels/{channel}/cheers` | `{"user":"alice","bits":100,"message":"gg"}` |
| POST | `/v1/channels/{channel}/subscriptions` | `{"user":"alice","tier":1}` |
| POST | `/v1/channels/{channel}/gifts` | `{"user":"alice","tier":1,"count":5}` or `{"user":"alice","tier":1,"recipients":["bob"]}` |
| GET | `/v1/channels/{channel}/subscriptions/{user}` | |
| GET | `/v1/channels/{channel}/hypetrain` | |
| GET | `/v1/channels/{channel}/leaderboard` | |
| GET | `/v1/channels/{channel}/events?user=alice` | Server-Sent Events stream |

All POSTs need `Content-Type: application/json` and `Idempotency-Key: <8-100 chars>`.

| Status | Meaning |
|---|---|
| 201 | Created |
| 200 | Replayed (same idempotency key) |
| 400 | Invalid input |
| 402 | Not enough Bits |
| 409 | Not enough eligible viewers, or recipient already subscribed |
| 422 | Idempotency key reused with a different request |

Example:

```bash
curl -X POST https://YOUR-API/v1/bits/purchases \
  -H "Content-Type: application/json" -H "Idempotency-Key: buy-0001" \
  -d '{"user":"alice","amount":1500}'
```

## Run the tests

GitHub Actions runs them on every push (see the Actions tab). Locally, with Go 1.22+:

```bash
cd backend
go test -race ./...
go run ./cmd/server
```

## Deploy (free)

Render: New Web Service, connect this repo, Root Directory `backend`, Runtime Docker. Render sets `PORT`. Set `ALLOWED_ORIGIN` to the frontend URL once it exists.

## Roadmap

- [x] Phase 1: Go backend, Hype Train engine, ledger, idempotency, live events, tests, CI
- [ ] Phase 2: PostgreSQL storage, React viewer page and creator dashboard
- [ ] Phase 3: Email and browser push notifications
- [ ] Phase 4: k6 load test with concurrent viewers, AWS deployment notes

Phase 1 keeps data in memory, so a restart clears it. User identity is a field in the request; real authentication comes with the frontend.
