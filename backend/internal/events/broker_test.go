package events

import "testing"

func TestPublishReachesOnlyThatChannel(t *testing.T) {
	b := NewBroker(4)
	a := b.Subscribe("one", "alice")
	other := b.Subscribe("two", "bob")
	b.Publish(Event{Type: "test", Channel: "one"})

	if got := <-a.C; got.Type != "test" {
		t.Fatalf("got %+v", got)
	}
	select {
	case ev := <-other.C:
		t.Fatalf("other channel should get nothing, got %+v", ev)
	default:
	}
}

func TestSlowViewerDoesNotBlock(t *testing.T) {
	b := NewBroker(1)
	s := b.Subscribe("one", "alice")
	for i := 0; i < 10; i++ {
		b.Publish(Event{Type: "spam", Channel: "one"}) // must not hang
	}
	if len(s.C) != 1 {
		t.Fatalf("buffer should hold exactly 1 event, has %d", len(s.C))
	}
}

func TestViewersAreDistinctAndRemovedOnUnsubscribe(t *testing.T) {
	b := NewBroker(1)
	s1 := b.Subscribe("one", "alice")
	b.Subscribe("one", "alice") // second tab
	b.Subscribe("one", "bob")
	b.Subscribe("one", "") // signed-out viewer
	if n := len(b.Viewers("one")); n != 2 {
		t.Fatalf("want 2 distinct viewers, got %d", n)
	}
	b.Unsubscribe(s1)
	b.Unsubscribe(s1) // safe to call twice
	if b.Count("one") != 3 {
		t.Fatalf("want 3 connections left, got %d", b.Count("one"))
	}
}
