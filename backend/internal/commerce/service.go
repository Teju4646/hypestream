// Package commerce handles Bits, cheers, subscriptions and gift subs.
//
// Rules every operation follows:
//  1. Validate the request.
//  2. Check the idempotency key: a repeated key returns the first receipt
//     and charges nothing again.
//  3. Change balances and append to the ledger in one locked step, so a
//     wallet never goes negative and the ledger always matches balances.
//  4. Feed points to the Hype Train, update the leaderboard, broadcast
//     events and send notifications.
package commerce

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/hypestream/backend/internal/events"
	"github.com/hypestream/backend/internal/hypetrain"
)

// Errors the API layer maps to HTTP status codes.
var (
	ErrInvalid             = errors.New("invalid request")
	ErrInsufficientBits    = errors.New("not enough Bits")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used with a different request")
	ErrNotEnoughViewers    = errors.New("not enough eligible viewers for this gift")
	ErrAlreadySubscribed   = errors.New("recipient already has an active subscription")
)

// SubPeriod is how long one month of subscription lasts.
const SubPeriod = 30 * 24 * time.Hour

// BitsPacks are the amounts a viewer can buy (test money only).
var BitsPacks = map[int]bool{100: true, 500: true, 1500: true, 5000: true, 10000: true}

var loginRe = regexp.MustCompile(`^[a-z0-9_]{3,25}$`)

// Tier is a subscription tier: 1, 2 or 3.
type Tier int

// Points is how much one sub of this tier adds to the Hype Train.
func (t Tier) Points() int {
	switch t {
	case 2:
		return 1000
	case 3:
		return 2500
	default:
		return 500
	}
}

func (t Tier) valid() bool { return t >= 1 && t <= 3 }

// EntryType labels a ledger entry.
type EntryType string

const (
	EntryBitsPurchase EntryType = "bits_purchase"
	EntryCheer        EntryType = "cheer"
	EntrySubscription EntryType = "subscription"
	EntryGiftSub      EntryType = "gift_sub"
)

// LedgerEntry is one append-only money movement. Entries are never edited.
type LedgerEntry struct {
	ID        int64     `json:"id"`
	At        time.Time `json:"at"`
	Type      EntryType `json:"type"`
	User      string    `json:"user"`
	Channel   string    `json:"channel,omitempty"`
	Recipient string    `json:"recipient,omitempty"`
	BitsDelta int       `json:"bitsDelta"`
	Tier      Tier      `json:"tier,omitempty"`
	Points    int       `json:"points"`
}

// Subscription is one viewer's sub to one channel.
type Subscription struct {
	User      string    `json:"user"`
	Channel   string    `json:"channel"`
	Tier      Tier      `json:"tier"`
	GiftedBy  string    `json:"giftedBy,omitempty"`
	StartedAt time.Time `json:"startedAt"`
	ExpiresAt time.Time `json:"expiresAt"`
	Months    int       `json:"months"`
}

// Receipt is returned for every successful operation.
type Receipt struct {
	Type        EntryType `json:"type"`
	User        string    `json:"user"`
	Channel     string    `json:"channel,omitempty"`
	EntryIDs    []int64   `json:"entryIds"`
	Points      int       `json:"points"`
	BitsBalance int       `json:"bitsBalance"`
	Recipients  []string  `json:"recipients,omitempty"`
	Replayed    bool      `json:"replayed"`
}

// Supporter is one row of a channel leaderboard.
type Supporter struct {
	User   string `json:"user"`
	Points int    `json:"points"`
}

// Notification is sent to a user (email/push arrive in Phase 3).
type Notification struct {
	To      string
	Kind    string
	Message string
}

// Notifier delivers notifications.
type Notifier interface{ Notify(Notification) }

// Broker is the part of events.Broker the service needs.
type Broker interface {
	Publish(events.Event)
	Viewers(channel string) []string
}

type idemRecord struct {
	fingerprint string
	receipt     Receipt
}

// Service is safe for concurrent use.
type Service struct {
	mu          sync.Mutex
	now         func() time.Time
	train       *hypetrain.Engine
	broker      Broker
	notifier    Notifier
	wallets     map[string]int
	subs        map[string]*Subscription // key: channel + "/" + user
	ledger      []LedgerEntry
	idem        map[string]idemRecord // key: user + "/" + idempotency key
	leaderboard map[string]map[string]int
	nextID      int64
}

// NewService wires the service. Pass nil for now to use the real clock.
func NewService(train *hypetrain.Engine, broker Broker, notifier Notifier, now func() time.Time) *Service {
	if now == nil {
		now = time.Now
	}
	return &Service{
		now: now, train: train, broker: broker, notifier: notifier,
		wallets: map[string]int{}, subs: map[string]*Subscription{},
		idem: map[string]idemRecord{}, leaderboard: map[string]map[string]int{},
	}
}

// ---------- requests ----------

// BuyBitsRequest tops up a wallet with test Bits.
type BuyBitsRequest struct {
	User           string `json:"user"`
	Amount         int    `json:"amount"`
	IdempotencyKey string `json:"-"`
}

// CheerRequest spends Bits in a channel.
type CheerRequest struct {
	User           string `json:"user"`
	Channel        string `json:"-"`
	Bits           int    `json:"bits"`
	Message        string `json:"message"`
	IdempotencyKey string `json:"-"`
}

// SubscribeRequest starts or renews the user's own sub.
type SubscribeRequest struct {
	User           string `json:"user"`
	Channel        string `json:"-"`
	Tier           Tier   `json:"tier"`
	IdempotencyKey string `json:"-"`
}

// GiftRequest gifts subs. Leave Recipients empty to gift to random viewers.
type GiftRequest struct {
	User           string   `json:"user"`
	Channel        string   `json:"-"`
	Tier           Tier     `json:"tier"`
	Count          int      `json:"count"`
	Recipients     []string `json:"recipients"`
	IdempotencyKey string   `json:"-"`
}

// ---------- operations ----------

// BuyBits adds Bits to a wallet.
func (s *Service) BuyBits(r BuyBitsRequest) (Receipt, error) {
	if err := checkLogin(r.User); err != nil {
		return Receipt{}, err
	}
	if !BitsPacks[r.Amount] {
		return Receipt{}, fmt.Errorf("%w: amount must be one of 100, 500, 1500, 5000, 10000", ErrInvalid)
	}
	fp := fmt.Sprintf("buy|%d", r.Amount)
	return s.run(r.User, r.IdempotencyKey, fp, func() (Receipt, error) {
		s.wallets[r.User] += r.Amount
		e := s.append(LedgerEntry{Type: EntryBitsPurchase, User: r.User, BitsDelta: r.Amount})
		return Receipt{Type: EntryBitsPurchase, User: r.User, EntryIDs: []int64{e.ID}, BitsBalance: s.wallets[r.User]}, nil
	})
}

// Cheer spends Bits in a channel. 1 Bit = 1 Hype Train point.
func (s *Service) Cheer(r CheerRequest) (Receipt, error) {
	if err := checkLogin(r.User); err != nil {
		return Receipt{}, err
	}
	if err := checkLogin(r.Channel); err != nil {
		return Receipt{}, err
	}
	if r.Bits < 1 || r.Bits > 10000 {
		return Receipt{}, fmt.Errorf("%w: bits must be between 1 and 10000", ErrInvalid)
	}
	if len(r.Message) > 300 {
		return Receipt{}, fmt.Errorf("%w: message is too long", ErrInvalid)
	}
	fp := fmt.Sprintf("cheer|%s|%d|%s", r.Channel, r.Bits, r.Message)
	return s.run(r.User, r.IdempotencyKey, fp, func() (Receipt, error) {
		if s.wallets[r.User] < r.Bits {
			return Receipt{}, ErrInsufficientBits
		}
		s.wallets[r.User] -= r.Bits
		e := s.append(LedgerEntry{Type: EntryCheer, User: r.User, Channel: r.Channel, BitsDelta: -r.Bits, Points: r.Bits})
		s.support(r.Channel, r.User, r.Bits, events.Event{
			Type: "cheer", Channel: r.Channel, User: r.User,
			Data: map[string]any{"bits": r.Bits, "message": r.Message},
		})
		return Receipt{Type: EntryCheer, User: r.User, Channel: r.Channel, EntryIDs: []int64{e.ID},
			Points: r.Bits, BitsBalance: s.wallets[r.User]}, nil
	})
}

// Subscribe starts a sub, or extends it by one month if already active.
func (s *Service) Subscribe(r SubscribeRequest) (Receipt, error) {
	if err := checkLogin(r.User); err != nil {
		return Receipt{}, err
	}
	if err := checkLogin(r.Channel); err != nil {
		return Receipt{}, err
	}
	if r.User == r.Channel {
		return Receipt{}, fmt.Errorf("%w: you can't subscribe to yourself", ErrInvalid)
	}
	if !r.Tier.valid() {
		return Receipt{}, fmt.Errorf("%w: tier must be 1, 2 or 3", ErrInvalid)
	}
	fp := fmt.Sprintf("sub|%s|%d", r.Channel, r.Tier)
	return s.run(r.User, r.IdempotencyKey, fp, func() (Receipt, error) {
		sub := s.grant(r.Channel, r.User, r.Tier, "")
		e := s.append(LedgerEntry{Type: EntrySubscription, User: r.User, Channel: r.Channel, Tier: r.Tier, Points: r.Tier.Points()})
		s.support(r.Channel, r.User, r.Tier.Points(), events.Event{
			Type: "subscription", Channel: r.Channel, User: r.User,
			Data: map[string]any{"tier": r.Tier, "months": sub.Months},
		})
		return Receipt{Type: EntrySubscription, User: r.User, Channel: r.Channel, EntryIDs: []int64{e.ID},
			Points: r.Tier.Points(), BitsBalance: s.wallets[r.User]}, nil
	})
}

// GiftSubs gifts subs to named recipients, or to random eligible viewers.
func (s *Service) GiftSubs(r GiftRequest) (Receipt, error) {
	if err := checkLogin(r.User); err != nil {
		return Receipt{}, err
	}
	if err := checkLogin(r.Channel); err != nil {
		return Receipt{}, err
	}
	if !r.Tier.valid() {
		return Receipt{}, fmt.Errorf("%w: tier must be 1, 2 or 3", ErrInvalid)
	}
	if len(r.Recipients) > 0 {
		r.Count = len(r.Recipients)
	}
	if r.Count < 1 || r.Count > 100 {
		return Receipt{}, fmt.Errorf("%w: count must be between 1 and 100", ErrInvalid)
	}
	seen := map[string]bool{}
	for _, u := range r.Recipients {
		if err := checkLogin(u); err != nil {
			return Receipt{}, err
		}
		if u == r.User || u == r.Channel || seen[u] {
			return Receipt{}, fmt.Errorf("%w: recipients must be unique and can't be you or the streamer", ErrInvalid)
		}
		seen[u] = true
	}
	fp := fmt.Sprintf("gift|%s|%d|%d|%v", r.Channel, r.Tier, r.Count, r.Recipients)
	return s.run(r.User, r.IdempotencyKey, fp, func() (Receipt, error) {
		recipients := r.Recipients
		if len(recipients) == 0 {
			// Community gift: random viewers who are watching and not subscribed.
			var eligible []string
			for _, v := range s.broker.Viewers(r.Channel) {
				if v != r.User && v != r.Channel && !s.active(r.Channel, v) {
					eligible = append(eligible, v)
				}
			}
			if len(eligible) < r.Count {
				return Receipt{}, fmt.Errorf("%w: %d eligible, %d requested", ErrNotEnoughViewers, len(eligible), r.Count)
			}
			rand.Shuffle(len(eligible), func(i, j int) { eligible[i], eligible[j] = eligible[j], eligible[i] })
			recipients = eligible[:r.Count]
		} else {
			for _, u := range recipients {
				if s.active(r.Channel, u) {
					return Receipt{}, fmt.Errorf("%w: %s", ErrAlreadySubscribed, u)
				}
			}
		}
		sort.Strings(recipients)

		ids := make([]int64, 0, len(recipients))
		for _, u := range recipients {
			s.grant(r.Channel, u, r.Tier, r.User)
			e := s.append(LedgerEntry{Type: EntryGiftSub, User: r.User, Channel: r.Channel, Recipient: u, Tier: r.Tier, Points: r.Tier.Points()})
			ids = append(ids, e.ID)
			s.notify(Notification{To: u, Kind: "gift_received", Message: fmt.Sprintf("%s gifted you a sub to %s", r.User, r.Channel)})
		}
		points := r.Tier.Points() * len(recipients)
		s.notify(Notification{To: r.User, Kind: "gift_receipt", Message: fmt.Sprintf("You gifted %d subs to %s's community", len(recipients), r.Channel)})
		s.support(r.Channel, r.User, points, events.Event{
			Type: "gift", Channel: r.Channel, User: r.User,
			Data: map[string]any{"count": len(recipients), "tier": r.Tier, "recipients": recipients},
		})
		return Receipt{Type: EntryGiftSub, User: r.User, Channel: r.Channel, EntryIDs: ids,
			Points: points, BitsBalance: s.wallets[r.User], Recipients: recipients}, nil
	})
}

// ---------- reads ----------

// Balance returns a user's Bits.
func (s *Service) Balance(user string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wallets[user]
}

// Ledger returns every entry where the user paid or received.
func (s *Service) Ledger(user string) []LedgerEntry {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []LedgerEntry{}
	for _, e := range s.ledger {
		if e.User == user || e.Recipient == user {
			out = append(out, e)
		}
	}
	return out
}

// Subscription returns the user's sub to a channel, if any.
func (s *Service) Subscription(channel, user string) (Subscription, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub, ok := s.subs[channel+"/"+user]
	if !ok {
		return Subscription{}, false
	}
	return *sub, true
}

// Leaderboard returns the top n supporters of a channel by points.
func (s *Service) Leaderboard(channel string, n int) []Supporter {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Supporter{}
	for u, p := range s.leaderboard[channel] {
		out = append(out, Supporter{User: u, Points: p})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Points != out[j].Points {
			return out[i].Points > out[j].Points
		}
		return out[i].User < out[j].User
	})
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out
}

// ---------- internals (call with s.mu held) ----------

// run applies idempotency around an operation. A failed operation does not
// store its key, so the client can safely retry it.
func (s *Service) run(user, key, fingerprint string, op func() (Receipt, error)) (Receipt, error) {
	if len(key) < 8 || len(key) > 100 {
		return Receipt{}, fmt.Errorf("%w: Idempotency-Key header must be 8 to 100 characters", ErrInvalid)
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	k := user + "/" + key
	if rec, ok := s.idem[k]; ok {
		if rec.fingerprint != fingerprint {
			return Receipt{}, ErrIdempotencyMismatch
		}
		r := rec.receipt
		r.Replayed = true
		return r, nil
	}
	r, err := op()
	if err != nil {
		return Receipt{}, err
	}
	s.idem[k] = idemRecord{fingerprint: fingerprint, receipt: r}
	return r, nil
}

func (s *Service) append(e LedgerEntry) LedgerEntry {
	s.nextID++
	e.ID = s.nextID
	e.At = s.now().UTC()
	s.ledger = append(s.ledger, e)
	return e
}

func (s *Service) active(channel, user string) bool {
	sub, ok := s.subs[channel+"/"+user]
	return ok && s.now().Before(sub.ExpiresAt)
}

func (s *Service) grant(channel, user string, tier Tier, giftedBy string) *Subscription {
	now := s.now().UTC()
	key := channel + "/" + user
	sub, ok := s.subs[key]
	if ok && now.Before(sub.ExpiresAt) {
		sub.ExpiresAt = sub.ExpiresAt.Add(SubPeriod) // renewal stacks on top
		sub.Months++
		sub.Tier = tier
		return sub
	}
	months := 1
	if ok {
		months = sub.Months + 1 // came back after a lapse
	}
	sub = &Subscription{User: user, Channel: channel, Tier: tier, GiftedBy: giftedBy,
		StartedAt: now, ExpiresAt: now.Add(SubPeriod), Months: months}
	s.subs[key] = sub
	return sub
}

func (s *Service) support(channel, user string, points int, ev events.Event) {
	if s.leaderboard[channel] == nil {
		s.leaderboard[channel] = map[string]int{}
	}
	s.leaderboard[channel][user] += points
	s.broker.Publish(ev)
	for _, te := range s.train.Contribute(channel, user, points) {
		s.broker.Publish(events.Event{Type: string(te.Type), Channel: channel, Data: map[string]any{"level": te.Level}})
		if te.Type == hypetrain.Started {
			s.notify(Notification{To: channel + ":followers", Kind: "hypetrain_started", Message: "A Hype Train just started on " + channel})
		}
	}
}

func (s *Service) notify(n Notification) {
	if s.notifier != nil {
		s.notifier.Notify(n)
	}
}

func checkLogin(name string) error {
	if !loginRe.MatchString(name) {
		return fmt.Errorf("%w: %q must be 3-25 lowercase letters, digits or underscores", ErrInvalid, name)
	}
	return nil
}
