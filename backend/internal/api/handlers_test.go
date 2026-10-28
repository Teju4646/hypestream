package api

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hypestream/backend/internal/commerce"
	"github.com/hypestream/backend/internal/events"
	"github.com/hypestream/backend/internal/hypetrain"
)

func newTestServer(t *testing.T) *httptest.Server {
	log.SetOutput(io.Discard)
	train := hypetrain.NewEngine(hypetrain.DefaultConfig(), nil)
	broker := events.NewBroker(64)
	s := &Server{
		Commerce:  commerce.NewService(train, broker, nil, nil),
		Train:     train,
		Broker:    broker,
		Heartbeat: time.Second,
	}
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	return ts
}

func post(t *testing.T, url, key, body string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Idempotency-Key", key)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var out map[string]any
	json.NewDecoder(res.Body).Decode(&out)
	return res, out
}

func TestBuyAndCheerFlow(t *testing.T) {
	ts := newTestServer(t)
	res, _ := post(t, ts.URL+"/v1/bits/purchases", "buy-key-0001", `{"user":"alice","amount":500}`)
	if res.StatusCode != http.StatusCreated {
		t.Fatalf("buy: %d", res.StatusCode)
	}
	res, body := post(t, ts.URL+"/v1/channels/streamer/cheers", "cheer-key-001", `{"user":"alice","bits":200,"message":"gg"}`)
	if res.StatusCode != http.StatusCreated || body["bitsBalance"].(float64) != 300 {
		t.Fatalf("cheer: %d %v", res.StatusCode, body)
	}
	// Same key again: replayed, 200, not charged.
	res, body = post(t, ts.URL+"/v1/channels/streamer/cheers", "cheer-key-001", `{"user":"alice","bits":200,"message":"gg"}`)
	if res.StatusCode != http.StatusOK || body["replayed"] != true || body["bitsBalance"].(float64) != 300 {
		t.Fatalf("replay: %d %v", res.StatusCode, body)
	}
	res, _ = post(t, ts.URL+"/v1/channels/streamer/cheers", "cheer-key-002", `{"user":"alice","bits":5000}`)
	if res.StatusCode != http.StatusPaymentRequired {
		t.Fatalf("overspend: want 402, got %d", res.StatusCode)
	}
}

func TestErrorsMapToStatusCodes(t *testing.T) {
	ts := newTestServer(t)
	cases := []struct {
		path, key, body string
		want            int
	}{
		{"/v1/bits/purchases", "", `{"user":"alice","amount":100}`, 400},               // missing key
		{"/v1/bits/purchases", "abcdefgh", `{"user":"alice","amount":3}`, 400},         // bad pack
		{"/v1/bits/purchases", "abcdefgh", `{"user":"alice","amount":100,"x":1}`, 400}, // unknown field
		{"/v1/channels/streamer/gifts", "gift-key-01", `{"user":"alice","tier":1,"count":2}`, 409},
	}
	for _, c := range cases {
		res, _ := post(t, ts.URL+c.path, c.key, c.body)
		if res.StatusCode != c.want {
			t.Errorf("%s %s: want %d, got %d", c.path, c.body, c.want, res.StatusCode)
		}
	}
}

func TestViewerReceivesLiveEvents(t *testing.T) {
	ts := newTestServer(t)
	res, err := http.Get(ts.URL + "/v1/channels/streamer/events?user=watcher")
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type %q", ct)
	}

	lines := make(chan string, 100)
	go func() {
		sc := bufio.NewScanner(res.Body)
		for sc.Scan() {
			lines <- sc.Text()
		}
		close(lines)
	}()

	waitFor := func(prefix string) {
		t.Helper()
		deadline := time.After(3 * time.Second)
		for {
			select {
			case l := <-lines:
				if strings.HasPrefix(l, prefix) {
					return
				}
			case <-deadline:
				t.Fatalf("timed out waiting for %q", prefix)
			}
		}
	}

	waitFor("event: hypetrain.state")
	for i, u := range []string{"aaa", "bbb", "ccc"} {
		post(t, ts.URL+"/v1/channels/streamer/subscriptions", fmt.Sprintf("sub-key-%04d", i), `{"user":"`+u+`","tier":1}`)
	}
	waitFor("event: subscription")
	waitFor("event: hypetrain.started")
}
