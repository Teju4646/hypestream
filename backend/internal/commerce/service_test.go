package commerce

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/hypestream/backend/internal/events"
	"github.com/hypestream/backend/internal/hypetrain"
)

type recNotifier struct {
	mu   sync.Mutex
	sent []Notification
}

func (r *recNotifier) Notify(n Notification) {
	r.mu.Lock()
	r.sent = append(r.sent, n)
	r.mu.Unlock()
}

type setup struct {
	svc    *Service
	broker *events.Broker
	notes  *recNotifier
	now    *time.Time
}

func newSetup() setup {
	now := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	b := events.NewBroker(64)
	n := &recNotifier{}
	svc := NewService(hypetrain.NewEngine(hypetrain.DefaultConfig(), clock), b, n, clock)
	return setup{svc: svc, broker: b, notes: n, now: &now}
}

var keyN int

func key() string { keyN++; return fmt.Sprintf("test-key-%04d", keyN) }

func TestCheerSpendsBitsAndRejectsOverspend(t *testing.T) {
	s := newSetup()
	if _, err := s.svc.BuyBits(BuyBitsRequest{User: "alice", Amount: 500, IdempotencyKey: key()}); err != nil {
		t.Fatal(err)
	}
	r, err := s.svc.Cheer(CheerRequest{User: "alice", Channel: "streamer", Bits: 300, IdempotencyKey: key()})
	if err != nil {
		t.Fatal(err)
	}
	if r.BitsBalance != 200 {
		t.Fatalf("want 200 left, got %d", r.BitsBalance)
	}
	_, err = s.svc.Cheer(CheerRequest{User: "alice", Channel: "streamer", Bits: 300, IdempotencyKey: key()})
	if !errors.Is(err, ErrInsufficientBits) {
		t.Fatalf("want ErrInsufficientBits, got %v", err)
	}
	if s.svc.Balance("alice") != 200 {
		t.Fatal("failed cheer must not change the balance")
	}
}

func TestSameIdempotencyKeyChargesOnce(t *testing.T) {
	s := newSetup()
	s.svc.BuyBits(BuyBitsRequest{User: "alice", Amount: 1500, IdempotencyKey: key()})
	k := key()
	req := CheerRequest{User: "alice", Channel: "streamer", Bits: 100, IdempotencyKey: k}

	first, err := s.svc.Cheer(req)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.svc.Cheer(req) // user double-tapped
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || second.EntryIDs[0] != first.EntryIDs[0] {
		t.Fatalf("second call should replay the first receipt: %+v", second)
	}
	if s.svc.Balance("alice") != 1400 {
		t.Fatalf("charged twice: balance %d", s.svc.Balance("alice"))
	}

	req.Bits = 999
	if _, err := s.svc.Cheer(req); !errors.Is(err, ErrIdempotencyMismatch) {
		t.Fatalf("reusing a key with a different body must fail, got %v", err)
	}
}

func TestConcurrentDoubleTapChargesOnce(t *testing.T) {
	s := newSetup()
	s.svc.BuyBits(BuyBitsRequest{User: "alice", Amount: 1500, IdempotencyKey: key()})
	k := key()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.svc.Cheer(CheerRequest{User: "alice", Channel: "streamer", Bits: 100, IdempotencyKey: k})
		}()
	}
	wg.Wait()
	if s.svc.Balance("alice") != 1400 {
		t.Fatalf("50 parallel retries must charge once, balance %d", s.svc.Balance("alice"))
	}
}

func TestConcurrentCheersNeverOverspend(t *testing.T) {
	s := newSetup()
	s.svc.BuyBits(BuyBitsRequest{User: "alice", Amount: 1500, IdempotencyKey: key()})
	var wg sync.WaitGroup
	var mu sync.Mutex
	ok := 0
	keys := make([]string, 100)
	for i := range keys {
		keys[i] = key()
	}
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(k string) {
			defer wg.Done()
			if _, err := s.svc.Cheer(CheerRequest{User: "alice", Channel: "streamer", Bits: 100, IdempotencyKey: k}); err == nil {
				mu.Lock()
				ok++
				mu.Unlock()
			}
		}(keys[i])
	}
	wg.Wait()
	if ok != 15 || s.svc.Balance("alice") != 0 {
		t.Fatalf("want exactly 15 successful cheers and 0 left, got %d and %d", ok, s.svc.Balance("alice"))
	}
}

func TestLedgerMatchesWallet(t *testing.T) {
	s := newSetup()
	s.svc.BuyBits(BuyBitsRequest{User: "alice", Amount: 5000, IdempotencyKey: key()})
	s.svc.Cheer(CheerRequest{User: "alice", Channel: "streamer", Bits: 1234, IdempotencyKey: key()})
	s.svc.Cheer(CheerRequest{User: "alice", Channel: "other_one", Bits: 66, IdempotencyKey: key()})
	sum := 0
	for _, e := range s.svc.Ledger("alice") {
		sum += e.BitsDelta
	}
	if sum != s.svc.Balance("alice") || sum != 3700 {
		t.Fatalf("ledger sum %d must equal balance %d", sum, s.svc.Balance("alice"))
	}
}

func TestSubscriptionRenewalStacks(t *testing.T) {
	s := newSetup()
	s.svc.Subscribe(SubscribeRequest{User: "alice", Channel: "streamer", Tier: 1, IdempotencyKey: key()})
	s.svc.Subscribe(SubscribeRequest{User: "alice", Channel: "streamer", Tier: 1, IdempotencyKey: key()})
	sub, ok := s.svc.Subscription("streamer", "alice")
	if !ok || sub.Months != 2 || !sub.ExpiresAt.Equal(s.now.Add(2*SubPeriod)) {
		t.Fatalf("unexpected sub %+v", sub)
	}
}

func TestCommunityGiftPicksOnlyEligibleViewers(t *testing.T) {
	s := newSetup()
	for _, v := range []string{"gifter", "bob", "carol", "dave", "subbed"} {
		s.broker.Subscribe("streamer", v)
	}
	s.svc.Subscribe(SubscribeRequest{User: "subbed", Channel: "streamer", Tier: 1, IdempotencyKey: key()})

	r, err := s.svc.GiftSubs(GiftRequest{User: "gifter", Channel: "streamer", Tier: 1, Count: 3, IdempotencyKey: key()})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"bob", "carol", "dave"}
	if fmt.Sprint(r.Recipients) != fmt.Sprint(want) {
		t.Fatalf("recipients %v, want %v", r.Recipients, want)
	}
	if r.Points != 1500 {
		t.Fatalf("want 1500 points, got %d", r.Points)
	}

	_, err = s.svc.GiftSubs(GiftRequest{User: "gifter", Channel: "streamer", Tier: 1, Count: 1, IdempotencyKey: key()})
	if !errors.Is(err, ErrNotEnoughViewers) {
		t.Fatalf("everyone is subbed now, want ErrNotEnoughViewers, got %v", err)
	}
}

func TestGiftSendsNotifications(t *testing.T) {
	s := newSetup()
	_, err := s.svc.GiftSubs(GiftRequest{User: "gifter", Channel: "streamer", Tier: 1, Recipients: []string{"bob", "carol"}, IdempotencyKey: key()})
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[string]int{}
	for _, n := range s.notes.sent {
		kinds[n.Kind]++
	}
	if kinds["gift_received"] != 2 || kinds["gift_receipt"] != 1 {
		t.Fatalf("unexpected notifications %v", kinds)
	}
}

func TestSupportStartsHypeTrainAndBroadcasts(t *testing.T) {
	s := newSetup()
	viewer := s.broker.Subscribe("streamer", "watcher")
	for _, u := range []string{"aaa", "bbb", "ccc"} {
		s.svc.Subscribe(SubscribeRequest{User: u, Channel: "streamer", Tier: 1, IdempotencyKey: key()})
	}
	started := false
	for len(viewer.C) > 0 {
		if ev := <-viewer.C; ev.Type == string(hypetrain.Started) {
			started = true
		}
	}
	if !started {
		t.Fatal("viewer should receive hypetrain.started")
	}
	top := s.svc.Leaderboard("streamer", 10)
	if len(top) != 3 || top[0].Points != 500 {
		t.Fatalf("unexpected leaderboard %+v", top)
	}
}

func TestValidation(t *testing.T) {
	s := newSetup()
	cases := []error{
		func() error {
			_, e := s.svc.BuyBits(BuyBitsRequest{User: "alice", Amount: 7, IdempotencyKey: key()})
			return e
		}(),
		func() error {
			_, e := s.svc.BuyBits(BuyBitsRequest{User: "Bad Name", Amount: 100, IdempotencyKey: key()})
			return e
		}(),
		func() error {
			_, e := s.svc.BuyBits(BuyBitsRequest{User: "alice", Amount: 100, IdempotencyKey: "short"})
			return e
		}(),
		func() error {
			_, e := s.svc.Subscribe(SubscribeRequest{User: "alice", Channel: "alice", Tier: 1, IdempotencyKey: key()})
			return e
		}(),
		func() error {
			_, e := s.svc.Subscribe(SubscribeRequest{User: "alice", Channel: "streamer", Tier: 4, IdempotencyKey: key()})
			return e
		}(),
		func() error {
			_, e := s.svc.GiftSubs(GiftRequest{User: "alice", Channel: "streamer", Tier: 1, Recipients: []string{"bob", "bob"}, IdempotencyKey: key()})
			return e
		}(),
	}
	for i, err := range cases {
		if !errors.Is(err, ErrInvalid) {
			t.Errorf("case %d: want ErrInvalid, got %v", i, err)
		}
	}
}
