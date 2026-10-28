package hypetrain

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestEngine() (*Engine, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)}
	return NewEngine(DefaultConfig(), clk.now), clk
}

func hasEvent(events []Event, typ EventType, level int) bool {
	for _, e := range events {
		if e.Type == typ && e.Level == level {
			return true
		}
	}
	return false
}

func TestTrainStartsAfterThreeContributions(t *testing.T) {
	e, _ := newTestEngine()
	e.Contribute("ch", "a", 100)
	e.Contribute("ch", "b", 100)
	if e.State("ch").Active {
		t.Fatal("train should not start after 2 contributions")
	}
	events := e.Contribute("ch", "c", 100)
	if !hasEvent(events, Started, 1) {
		t.Fatalf("expected started event, got %v", events)
	}
	s := e.State("ch")
	if !s.Active || s.Level != 1 || s.Progress != 300 {
		t.Fatalf("unexpected state %+v", s)
	}
}

func TestOldContributionsFallOutOfWindow(t *testing.T) {
	e, clk := newTestEngine()
	e.Contribute("ch", "a", 100)
	e.Contribute("ch", "b", 100)
	clk.advance(6 * time.Minute)
	e.Contribute("ch", "c", 100)
	if e.State("ch").Active {
		t.Fatal("contributions older than the window must not start a train")
	}
}

func TestLevelUpCarriesLeftoverPoints(t *testing.T) {
	e, _ := newTestEngine()
	e.Contribute("ch", "a", 100)
	e.Contribute("ch", "b", 100)
	e.Contribute("ch", "c", 100) // level 1, 300 points
	// Goal(1)=1600, Goal(2)=2200. 300+3600 = 3900 -> level 3 with 100 left.
	events := e.Contribute("ch", "a", 3600)
	if !hasEvent(events, LevelUp, 2) || !hasEvent(events, LevelUp, 3) {
		t.Fatalf("expected two level ups, got %v", events)
	}
	s := e.State("ch")
	if s.Level != 3 || s.Progress != 100 {
		t.Fatalf("unexpected state %+v", s)
	}
	if s.Conductors[0].User != "a" || s.Conductors[0].Points != 3700 {
		t.Fatalf("top conductor should be a with 3700, got %+v", s.Conductors)
	}
}

func TestTrainEndsWhenTimerRunsOut(t *testing.T) {
	e, clk := newTestEngine()
	for _, u := range []string{"a", "b", "c"} {
		e.Contribute("ch", u, 100)
	}
	clk.advance(4 * time.Minute)
	e.Contribute("ch", "a", 100) // resets the timer
	clk.advance(4 * time.Minute)
	if !e.State("ch").Active {
		t.Fatal("contribution should have reset the timer")
	}
	clk.advance(2 * time.Minute)
	events := e.Expire()
	if !hasEvent(events, Ended, 1) {
		t.Fatalf("expected ended event, got %v", events)
	}
	if e.State("ch").Active {
		t.Fatal("train should be over")
	}
}

func TestChannelsAreIndependent(t *testing.T) {
	e, _ := newTestEngine()
	for _, u := range []string{"a", "b", "c"} {
		e.Contribute("one", u, 100)
	}
	e.Contribute("two", "a", 100)
	if !e.State("one").Active || e.State("two").Active {
		t.Fatal("channels must not share train state")
	}
}
