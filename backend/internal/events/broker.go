// Package events fans out live events to every viewer watching a channel.
//
// Each viewer connection gets its own buffered Go channel. Publish never
// blocks: if a slow viewer's buffer is full, that one event is dropped for
// that viewer only, so one bad connection can't slow down everyone else.
package events

import (
	"sync"
	"time"
)

// Event is what viewers receive over Server-Sent Events.
type Event struct {
	Type    string    `json:"type"`
	Channel string    `json:"channel"`
	User    string    `json:"user,omitempty"`
	Data    any       `json:"data,omitempty"`
	At      time.Time `json:"at"`
}

// Subscriber is one open viewer connection.
type Subscriber struct {
	C       chan Event
	channel string
	user    string
}

// Broker is safe for concurrent use.
type Broker struct {
	mu     sync.RWMutex
	subs   map[string]map[*Subscriber]struct{}
	buffer int
}

// NewBroker creates a broker; buffer is the queue size per viewer.
func NewBroker(buffer int) *Broker {
	return &Broker{subs: map[string]map[*Subscriber]struct{}{}, buffer: buffer}
}

// Subscribe registers a viewer on a channel. Call Unsubscribe when done.
func (b *Broker) Subscribe(channel, user string) *Subscriber {
	s := &Subscriber{C: make(chan Event, b.buffer), channel: channel, user: user}
	b.mu.Lock()
	if b.subs[channel] == nil {
		b.subs[channel] = map[*Subscriber]struct{}{}
	}
	b.subs[channel][s] = struct{}{}
	b.mu.Unlock()
	return s
}

// Unsubscribe removes a viewer and closes its Go channel.
func (b *Broker) Unsubscribe(s *Subscriber) {
	b.mu.Lock()
	if set, ok := b.subs[s.channel]; ok {
		if _, ok := set[s]; ok {
			delete(set, s)
			close(s.C)
		}
		if len(set) == 0 {
			delete(b.subs, s.channel)
		}
	}
	b.mu.Unlock()
}

// Publish sends an event to every viewer of ev.Channel.
func (b *Broker) Publish(ev Event) {
	if ev.At.IsZero() {
		ev.At = time.Now().UTC()
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs[ev.Channel] {
		select {
		case s.C <- ev:
		default: // viewer is too slow; skip rather than block everyone
		}
	}
}

// Viewers returns the distinct signed-in users watching a channel.
// Gift subs use this to pick random recipients.
func (b *Broker) Viewers(channel string) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	seen := map[string]bool{}
	var out []string
	for s := range b.subs[channel] {
		if s.user != "" && !seen[s.user] {
			seen[s.user] = true
			out = append(out, s.user)
		}
	}
	return out
}

// Count returns how many connections are open on a channel.
func (b *Broker) Count(channel string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subs[channel])
}
