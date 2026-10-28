// Command server runs the HypeStream API.
//
// Environment variables:
//
//	PORT            port to listen on (default 8080; Render sets this)
//	ALLOWED_ORIGIN  frontend URL allowed by CORS (default *)
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hypestream/backend/internal/api"
	"github.com/hypestream/backend/internal/commerce"
	"github.com/hypestream/backend/internal/events"
	"github.com/hypestream/backend/internal/hypetrain"
)

// logNotifier prints notifications. Phase 3 replaces it with email and push.
type logNotifier struct{}

func (logNotifier) Notify(n commerce.Notification) {
	log.Printf("notify to=%s kind=%s msg=%q", n.To, n.Kind, n.Message)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	train := hypetrain.NewEngine(hypetrain.DefaultConfig(), nil)
	broker := events.NewBroker(64)
	svc := commerce.NewService(train, broker, logNotifier{}, nil)

	srv := &http.Server{
		Addr: ":" + port,
		Handler: (&api.Server{
			Commerce:      svc,
			Train:         train,
			Broker:        broker,
			AllowedOrigin: os.Getenv("ALLOWED_ORIGIN"),
		}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Tell viewers when a train runs out of time, even if nobody contributes.
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for _, ev := range train.Expire() {
					broker.Publish(events.Event{Type: string(ev.Type), Channel: ev.Channel, Data: map[string]any{"level": ev.Level}})
				}
			}
		}
	}()

	go func() {
		log.Printf("HypeStream API listening on :%s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
	log.Print("shutting down")
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(shutdown)
}
