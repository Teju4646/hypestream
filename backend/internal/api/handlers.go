// Package api exposes the commerce service over HTTP.
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/hypestream/backend/internal/commerce"
	"github.com/hypestream/backend/internal/events"
	"github.com/hypestream/backend/internal/hypetrain"
)

// Server holds what the handlers need.
type Server struct {
	Commerce      *commerce.Service
	Train         *hypetrain.Engine
	Broker        *events.Broker
	AllowedOrigin string
	Heartbeat     time.Duration
}

// Routes returns the HTTP handler with every route registered.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("POST /v1/bits/purchases", s.buyBits)
	mux.HandleFunc("GET /v1/users/{user}/wallet", s.wallet)
	mux.HandleFunc("GET /v1/users/{user}/ledger", s.ledger)

	mux.HandleFunc("POST /v1/channels/{channel}/cheers", s.cheer)
	mux.HandleFunc("POST /v1/channels/{channel}/subscriptions", s.subscribe)
	mux.HandleFunc("POST /v1/channels/{channel}/gifts", s.gift)
	mux.HandleFunc("GET /v1/channels/{channel}/subscriptions/{user}", s.subscription)
	mux.HandleFunc("GET /v1/channels/{channel}/hypetrain", s.hypeTrain)
	mux.HandleFunc("GET /v1/channels/{channel}/leaderboard", s.leaderboard)
	mux.HandleFunc("GET /v1/channels/{channel}/events", s.stream)

	return s.cors(logRequests(mux))
}

// ---------- writes ----------

func (s *Server) buyBits(w http.ResponseWriter, r *http.Request) {
	var req commerce.BuyBitsRequest
	if !readJSON(w, r, &req) {
		return
	}
	req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	respond(w, http.StatusCreated)(s.Commerce.BuyBits(req))
}

func (s *Server) cheer(w http.ResponseWriter, r *http.Request) {
	var req commerce.CheerRequest
	if !readJSON(w, r, &req) {
		return
	}
	req.Channel = r.PathValue("channel")
	req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	respond(w, http.StatusCreated)(s.Commerce.Cheer(req))
}

func (s *Server) subscribe(w http.ResponseWriter, r *http.Request) {
	var req commerce.SubscribeRequest
	if !readJSON(w, r, &req) {
		return
	}
	req.Channel = r.PathValue("channel")
	req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	respond(w, http.StatusCreated)(s.Commerce.Subscribe(req))
}

func (s *Server) gift(w http.ResponseWriter, r *http.Request) {
	var req commerce.GiftRequest
	if !readJSON(w, r, &req) {
		return
	}
	req.Channel = r.PathValue("channel")
	req.IdempotencyKey = r.Header.Get("Idempotency-Key")
	respond(w, http.StatusCreated)(s.Commerce.GiftSubs(req))
}

// ---------- reads ----------

func (s *Server) wallet(w http.ResponseWriter, r *http.Request) {
	u := r.PathValue("user")
	writeJSON(w, http.StatusOK, map[string]any{"user": u, "bits": s.Commerce.Balance(u)})
}

func (s *Server) ledger(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"entries": s.Commerce.Ledger(r.PathValue("user"))})
}

func (s *Server) subscription(w http.ResponseWriter, r *http.Request) {
	sub, ok := s.Commerce.Subscription(r.PathValue("channel"), r.PathValue("user"))
	if !ok {
		writeError(w, http.StatusNotFound, "no subscription")
		return
	}
	writeJSON(w, http.StatusOK, sub)
}

func (s *Server) hypeTrain(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Train.State(r.PathValue("channel")))
}

func (s *Server) leaderboard(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"supporters": s.Commerce.Leaderboard(r.PathValue("channel"), 10)})
}

// stream sends live events with Server-Sent Events. A viewer opens
// /v1/channels/{channel}/events?user=name and keeps the connection open.
func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, http.StatusInternalServerError, "streaming not supported")
		return
	}
	channel := r.PathValue("channel")
	sub := s.Broker.Subscribe(channel, r.URL.Query().Get("user"))
	defer s.Broker.Unsubscribe(sub)

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	// First message: current train, so a new viewer sees the right bar.
	writeSSE(w, events.Event{Type: "hypetrain.state", Channel: channel, Data: s.Train.State(channel), At: time.Now().UTC()})
	flusher.Flush()

	beat := s.Heartbeat
	if beat <= 0 {
		beat = 20 * time.Second
	}
	ticker := time.NewTicker(beat)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev, open := <-sub.C:
			if !open {
				return
			}
			writeSSE(w, ev)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprint(w, ": ping\n\n") // keeps proxies from closing the connection
			flusher.Flush()
		}
	}
}

// ---------- helpers ----------

func writeSSE(w http.ResponseWriter, ev events.Event) {
	b, _ := json.Marshal(ev)
	fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b)
}

func readJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<16)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return false
	}
	return true
}

func respond(w http.ResponseWriter, okStatus int) func(commerce.Receipt, error) {
	return func(rec commerce.Receipt, err error) {
		if err != nil {
			writeError(w, statusFor(err), err.Error())
			return
		}
		status := okStatus
		if rec.Replayed {
			status = http.StatusOK
		}
		writeJSON(w, status, rec)
	}
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, commerce.ErrInvalid):
		return http.StatusBadRequest
	case errors.Is(err, commerce.ErrInsufficientBits):
		return http.StatusPaymentRequired
	case errors.Is(err, commerce.ErrIdempotencyMismatch):
		return http.StatusUnprocessableEntity
	case errors.Is(err, commerce.ErrNotEnoughViewers), errors.Is(err, commerce.ErrAlreadySubscribed):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (s *Server) cors(next http.Handler) http.Handler {
	origin := s.AllowedOrigin
	if origin == "" {
		origin = "*"
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Idempotency-Key")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// Flush lets SSE keep working through the logging wrapper.
func (w *statusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(sw, r)
		log.Printf("%s %s %d %s", r.Method, r.URL.Path, sw.status, time.Since(start).Round(time.Microsecond))
	})
}
