// Package hypetrain runs the Hype Train for every channel.
//
// How it works:
//   - A train starts when a channel gets StartContributions contributions
//     inside a sliding StartWindow (default: 3 contributions in 5 minutes).
//   - While active, every contribution adds points. When points reach the
//     level goal, the train levels up and the leftover points carry over.
//   - Every contribution resets the countdown. When the countdown runs out,
//     the train ends.
package hypetrain

import (
	"sort"
	"sync"
	"time"
)

// Config controls when a train starts and how long it lasts.
type Config struct {
	StartContributions int
	StartWindow        time.Duration
	Duration           time.Duration
}

// DefaultConfig is used by the server.
func DefaultConfig() Config {
	return Config{StartContributions: 3, StartWindow: 5 * time.Minute, Duration: 5 * time.Minute}
}

// Goal returns the points needed to finish a level. Each level is harder.
func Goal(level int) int {
	return 1000 + 600*level
}

// EventType names what happened to a train.
type EventType string

const (
	Started EventType = "hypetrain.started"
	LevelUp EventType = "hypetrain.level_up"
	Ended   EventType = "hypetrain.ended"
)

// Event is returned to the caller so it can be broadcast to viewers.
type Event struct {
	Type    EventType `json:"type"`
	Channel string    `json:"channel"`
	Level   int       `json:"level"`
}

// Conductor is a top contributor on the current train.
type Conductor struct {
	User   string `json:"user"`
	Points int    `json:"points"`
}

// State is a snapshot of one channel's train.
type State struct {
	Channel    string      `json:"channel"`
	Active     bool        `json:"active"`
	Level      int         `json:"level"`
	Progress   int         `json:"progress"`
	Goal       int         `json:"goal"`
	EndsAt     *time.Time  `json:"endsAt,omitempty"`
	Conductors []Conductor `json:"conductors"`
}

type contribution struct {
	at     time.Time
	user   string
	points int
}

type train struct {
	recent       []contribution // only used while the train is not active
	active       bool
	level        int
	progress     int
	endsAt       time.Time
	contributors map[string]int
}

// Engine is safe for concurrent use.
type Engine struct {
	mu       sync.Mutex
	cfg      Config
	now      func() time.Time
	channels map[string]*train
}

// NewEngine creates an engine. Pass nil for now to use the real clock.
func NewEngine(cfg Config, now func() time.Time) *Engine {
	if now == nil {
		now = time.Now
	}
	return &Engine{cfg: cfg, now: now, channels: map[string]*train{}}
}

func (e *Engine) get(channel string) *train {
	t, ok := e.channels[channel]
	if !ok {
		t = &train{contributors: map[string]int{}}
		e.channels[channel] = t
	}
	return t
}

// Contribute records support for a channel and returns any events it caused.
func (e *Engine) Contribute(channel, user string, points int) []Event {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := e.now()
	t := e.get(channel)
	var events []Event

	// A train whose timer ran out ends before this contribution is counted.
	if t.active && !now.Before(t.endsAt) {
		events = append(events, e.end(channel, t))
	}

	if !t.active {
		// Sliding window: drop contributions older than StartWindow.
		cutoff := now.Add(-e.cfg.StartWindow)
		kept := t.recent[:0]
		for _, c := range t.recent {
			if c.at.After(cutoff) {
				kept = append(kept, c)
			}
		}
		t.recent = append(kept, contribution{at: now, user: user, points: points})

		if len(t.recent) < e.cfg.StartContributions {
			return events
		}

		// Start the train. Contributions in the window count toward level 1.
		t.active = true
		t.level = 1
		t.progress = 0
		t.contributors = map[string]int{}
		t.endsAt = now.Add(e.cfg.Duration)
		events = append(events, Event{Type: Started, Channel: channel, Level: 1})
		for _, c := range t.recent {
			events = append(events, e.addPoints(channel, t, c.user, c.points)...)
		}
		t.recent = nil
		return events
	}

	t.endsAt = now.Add(e.cfg.Duration)
	return append(events, e.addPoints(channel, t, user, points)...)
}

func (e *Engine) addPoints(channel string, t *train, user string, points int) []Event {
	var events []Event
	t.contributors[user] += points
	t.progress += points
	for t.progress >= Goal(t.level) {
		t.progress -= Goal(t.level)
		t.level++
		events = append(events, Event{Type: LevelUp, Channel: channel, Level: t.level})
	}
	return events
}

func (e *Engine) end(channel string, t *train) Event {
	ev := Event{Type: Ended, Channel: channel, Level: t.level}
	t.active = false
	t.level = 0
	t.progress = 0
	t.contributors = map[string]int{}
	t.recent = nil
	return ev
}

// Expire ends every train whose timer has run out. The server calls it
// once a second so viewers learn a train ended even when nobody is
// contributing.
func (e *Engine) Expire() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	var events []Event
	for ch, t := range e.channels {
		if t.active && !now.Before(t.endsAt) {
			events = append(events, e.end(ch, t))
		}
	}
	return events
}

// State returns the current train for a channel.
func (e *Engine) State(channel string) State {
	e.mu.Lock()
	defer e.mu.Unlock()
	t := e.get(channel)
	s := State{Channel: channel, Conductors: []Conductor{}}
	if !t.active || !e.now().Before(t.endsAt) {
		return s
	}
	ends := t.endsAt
	s.Active = true
	s.Level = t.level
	s.Progress = t.progress
	s.Goal = Goal(t.level)
	s.EndsAt = &ends
	for u, p := range t.contributors {
		s.Conductors = append(s.Conductors, Conductor{User: u, Points: p})
	}
	sort.Slice(s.Conductors, func(i, j int) bool {
		if s.Conductors[i].Points != s.Conductors[j].Points {
			return s.Conductors[i].Points > s.Conductors[j].Points
		}
		return s.Conductors[i].User < s.Conductors[j].User
	})
	if len(s.Conductors) > 3 {
		s.Conductors = s.Conductors[:3]
	}
	return s
}
