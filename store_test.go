package main

// Tests for the store. The one that matters most is the retirement test: a new
// session must not inherit its predecessor's mail. Everything else here is
// ordinary bookkeeping.

import (
	"path/filepath"
	"testing"
	"time"
)

// testStore opens a store in a temp directory.
func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := OpenStore(filepath.Join(t.TempDir(), "starling.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// agent is the identity a pod named n verifies as.
func agent(n string) Identity {
	return Identity{
		Pod:            n,
		UID:            "uid-" + n,
		ServiceAccount: "system:serviceaccount:local:default",
	}
}

// Claiming a channel makes it the pod's current one, recorded with the
// pod's identity.
func TestClaimingAChannelMakesItThePodsCurrent(t *testing.T) {
	s := testStore(t)
	info, err := s.ClaimChannel(agent("agent-0"), "first session")
	if err != nil {
		t.Fatal(err)
	}
	got, err := s.CurrentChannel("agent-0")
	if err != nil {
		t.Fatal(err)
	}
	if got.Channel != info.Channel {
		t.Errorf(
			"current channel = %q, want %q",
			got.Channel,
			info.Channel,
		)
	}
	if got.Pod != "agent-0" || got.UID != "uid-agent-0" {
		t.Errorf("identity not recorded: %+v", got)
	}
}

// THE POINT OF CHANNELS. A pod outlives a session. Without retirement, the
// second occupant of agent-2 opens mail written to someone else -- the same
// inheritance that session directories already close on the filesystem
// side, arriving by post instead.
func TestANewSessionDoesNotInheritTheOldOnesMail(t *testing.T) {
	s := testStore(t)
	first, err := s.ClaimChannel(agent("agent-2"), "session one")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Deliver(first.Channel, []byte(`{"body":"for the first session"}`)); err != nil {
		t.Fatal(err)
	}

	second, err := s.ClaimChannel(agent("agent-2"), "session two")
	if err != nil {
		t.Fatal(err)
	}
	if second.Channel == first.Channel {
		t.Fatal(
			"a new claim must mint a new channel, not reuse the " +
				"pod's old one",
		)
	}

	msgs, err := s.Take(second.Channel, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 0 {
		t.Errorf(
			"the new session received %d inherited message(s); "+
				"want 0",
			len(msgs),
		)
	}

	// And addressing the pod now reaches the new session, not the old one.
	cur, err := s.CurrentChannel("agent-2")
	if err != nil {
		t.Fatal(err)
	}
	if cur.Channel != second.Channel {
		t.Errorf(
			"pod resolves to %q, want the new channel %q",
			cur.Channel,
			second.Channel,
		)
	}
}

// Messages delivered to a channel are taken back, in order.
func TestDeliverThenTake(t *testing.T) {
	s := testStore(t)
	ch, err := s.ClaimChannel(agent("agent-1"), "")
	if err != nil {
		t.Fatal(err)
	}
	for _, b := range []string{`{"n":1}`, `{"n":2}`, `{"n":3}`} {
		if err := s.Deliver(ch.Channel, []byte(b)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := s.Take(ch.Channel, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("took %d messages, want 3", len(got))
	}
	// Order is acceptance order, from bbolt's sequence rather than a clock
	// -- two messages accepted in the same millisecond still have an order.
	if string(got[0]) != `{"n":1}` || string(got[2]) != `{"n":3}` {
		t.Errorf("order not preserved: %s ... %s", got[0], got[2])
	}
}

// Delivery is at most once: a taken message is gone.
func TestTakeRemoves(t *testing.T) {
	s := testStore(t)
	ch, _ := s.ClaimChannel(agent("agent-1"), "")
	_ = s.Deliver(ch.Channel, []byte(`{"n":1}`))

	if _, err := s.Take(ch.Channel, 10); err != nil {
		t.Fatal(err)
	}
	again, err := s.Take(ch.Channel, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Errorf(
			"a taken message came back %d time(s); delivery is "+
				"at-most-once here",
			len(again),
		)
	}
}

// Take returns at most max messages and leaves the rest queued.
func TestTakeRespectsMax(t *testing.T) {
	s := testStore(t)
	ch, _ := s.ClaimChannel(agent("agent-1"), "")
	for i := 0; i < 10; i++ {
		_ = s.Deliver(ch.Channel, []byte(`{}`))
	}
	got, err := s.Take(ch.Channel, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Errorf("took %d, want 4", len(got))
	}
	depth, err := s.Depth(ch.Channel)
	if err != nil {
		t.Fatal(err)
	}
	if depth != 6 {
		t.Errorf("depth = %d after taking 4 of 10, want 6", depth)
	}
}

// Silently accepting mail for a nonexistent channel would mean messages
// that vanish with no error and no counter -- the worst available outcome,
// because it looks exactly like success.
func TestDeliveringToAChannelThatDoesNotExistFails(t *testing.T) {
	s := testStore(t)
	err := s.Deliver("ch_nothing", []byte(`{}`))
	if err == nil {
		t.Fatal("delivering to an unknown channel must fail")
	}
}

// "nobody by that name" and "nobody home right now" want different
// responses: one is a typo, the other is a dead agent.
func TestAddressingAPodWithNoSessionIsDistinguishable(t *testing.T) {
	s := testStore(t)
	_, err := s.CurrentChannel("agent-9")
	if err != ErrNoChannel {
		t.Errorf("err = %v, want ErrNoChannel", err)
	}
}

// A heartbeat moves the channel's last-seen time forward.
func TestHeartbeatMovesForward(t *testing.T) {
	s := testStore(t)
	now := time.Now()
	s.now = func() time.Time { return now }

	ch, err := s.ClaimChannel(agent("agent-3"), "")
	if err != nil {
		t.Fatal(err)
	}
	first := ch.LastHeartbeat

	now = now.Add(30 * time.Second)
	if err := s.Heartbeat(ch.Channel); err != nil {
		t.Fatal(err)
	}
	got, err := s.CurrentChannel("agent-3")
	if err != nil {
		t.Fatal(err)
	}
	if !got.LastHeartbeat.After(first) {
		t.Errorf(
			"heartbeat did not advance: %v then %v",
			first,
			got.LastHeartbeat,
		)
	}
}

// A heartbeat for a channel that does not exist says so.
func TestHeartbeatOnAnUnknownChannelFails(t *testing.T) {
	s := testStore(t)
	if err := s.Heartbeat("ch_nothing"); err != ErrNoChannel {
		t.Errorf("err = %v, want ErrNoChannel", err)
	}
}

// Listing exists to show which pollers have gone silent, so excluding the
// silent ones would defeat it entirely.
func TestLiveChannelsIncludesQuietOnes(t *testing.T) {
	s := testStore(t)
	if _, err := s.ClaimChannel(agent("agent-0"), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimChannel(agent("agent-1"), ""); err != nil {
		t.Fatal(err)
	}
	all, err := s.LiveChannels()
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Errorf("listed %d channels, want 2", len(all))
	}
}

// A fresh channel's inbox is empty.
func TestDepthOfAnEmptyInbox(t *testing.T) {
	s := testStore(t)
	ch, _ := s.ClaimChannel(agent("agent-0"), "")
	n, err := s.Depth(ch.Channel)
	if err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("depth = %d on a fresh channel, want 0", n)
	}
}

// Channel ids do not repeat in a thousand draws.
func TestChannelIDsDoNotRepeat(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := newChannelID()
		if seen[id] {
			t.Fatalf("channel id %q minted twice in 1000 draws", id)
		}
		seen[id] = true
	}
}
