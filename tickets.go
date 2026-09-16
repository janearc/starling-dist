package main

// Tickets: who may talk to whom, and for how long.
//
// A ticket is the pair of sessions, not the content. It answers "may these two
// exchange messages at all, right now" and nothing else -- what they may say is
// the size cap and the message shape, which are a different question answered
// in a different place.
//
// Keeping those two apart is deliberate: a mechanism that both admits a
// conversation and censors it becomes the place every argument about either
// lands.
//
// A ticket is NOT a credential the sender presents. This is the property worth
// protecting and it is easy to lose.
//
// If a ticket id were a field on a message, it would be a field a sender writes
// about itself -- which is the thing this whole service exists to make
// impossible, arriving by a side door and looking like session management.
//
// So starling looks a ticket up by the pair of channel ids it stamped: the
// sender's, established from its token, and the recipient's, resolved by
// starling. The id is returned to the requester as a handle for logs and for
// the operator, and there is nowhere to send it back.
//
// A payload carrying `ticket` fails to decode, like `from` does.
//
// Keyed on channels, NOT ON PODS. A pod outlives a session, so a ticket between
// pods would let the second occupant of a slot inherit a conversation it was
// never part of -- the same inheritance path the inbox already closes. Between
// channels, either end restarting ends the ticket, which is the honest reading:
//
// the peer that agreed to talk is gone.
//
// Opened unilaterally, NOT negotiated. The requester opens the ticket and the
// peer is not asked.
//
// A handshake would need the peer to be executing, and an idle Claude session
// is blocked on a read with no loop and no timer -- so consent could only ever
// come from an agent that was already busy, which is exactly the one you do not
// need to interrupt.
//
// A ticket grants reachability, not attention: the peer still has to read its
// mail, and does not have to answer.

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

// pairKey(a, b) -> ticket json. One entry per pair of sessions.
var bucketTickets = []byte("tickets")

// How long a ticket survives with no message on it. Thirty minutes is long
// enough that a pair working through something does not have to keep asking,
// and short enough that a conversation nobody came back to is closed rather
// than left standing until one of the pods restarts.
const ticketIdleTTL = 30 * time.Minute

var (
	// ErrNoTicket means these two sessions have never been introduced.
	ErrNoTicket = errors.New("no ticket between those two sessions")
	// ErrTicketExpired means they were, and then nobody said anything for
	// the idle period. Distinct from ErrNoTicket for the same reason a typo
	// and a dead agent are distinct: one caller needs to ask, the other
	// needs to know its conversation lapsed.
	ErrTicketExpired = errors.New("the ticket closed after being idle")
)

// Ticket is permission for two sessions to exchange messages.
//
// Both ends are channel ids, held sorted so that the pair has one key rather
// than two, and a ticket opened in one direction works in both -- a channel
// between two agents is not a one-way street, and issuing two half-tickets
// would mean an agent could be talked at without being able to reply.
type Ticket struct {
	ID string `json:"id"`

	// The two channels, sorted. The identity that matters.
	A string `json:"a"`
	B string `json:"b"`

	// The pods those channels belong to, for the log and the operator. Not
	// identity here -- the channels are -- and never used for lookup.
	APod string `json:"a_pod"`
	BPod string `json:"b_pod"`

	OpenedAt time.Time `json:"opened_at"`
	LastUsed time.Time `json:"last_used"`
}

// expiredAt reports whether this ticket has been idle past the TTL.
func (t Ticket) expiredAt(now time.Time) bool {
	return now.Sub(t.LastUsed) > ticketIdleTTL
}

// holds reports whether a channel is one of this ticket's two ends.
func (t Ticket) holds(channel string) bool {
	return t.A == channel || t.B == channel
}

// pairKey renders two channel ids as one bucket key, sorted so that the pair
// keys the same whichever end asks.
func pairKey(a, b string) []byte {
	if a > b {
		a, b = b, a
	}
	return []byte(a + "|" + b)
}

// OpenTicket opens a ticket between two sessions, or restarts the idle clock on
// the one they already have.
//
// Re-requesting is not an error and does not mint a second ticket: an agent
// that asks again because it lost track is asking for the same thing it already
// has, and answering with a fresh id would leave two records for one pair.
func (s *Store) OpenTicket(a, b ChannelInfo) (Ticket, error) {
	now := s.now().UTC()
	tk := Ticket{
		ID:       newTicketID(),
		A:        a.Channel,
		B:        b.Channel,
		APod:     a.Pod,
		BPod:     b.Pod,
		OpenedAt: now,
		LastUsed: now,
	}
	if tk.A > tk.B {
		tk.A, tk.B = tk.B, tk.A
		tk.APod, tk.BPod = tk.BPod, tk.APod
	}

	key := pairKey(a.Channel, b.Channel)
	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketTickets)
		if raw := bkt.Get(key); raw != nil {
			var existing Ticket
			if err := json.Unmarshal(raw, &existing); err != nil {
				return err
			}
			// An expired ticket is replaced rather than revived, so
			// its OpenedAt tells the truth about when this
			// conversation started.
			if !existing.expiredAt(now) {
				existing.LastUsed = now
				tk = existing
			}
		}
		enc, err := json.Marshal(tk)
		if err != nil {
			return err
		}
		return bkt.Put(key, enc)
	})
	if err != nil {
		return Ticket{}, fmt.Errorf(
			"opening a ticket between %s and %s: %w",
			a.Pod,
			b.Pod,
			err,
		)
	}
	return tk, nil
}

// UseTicket checks that two sessions may talk and restarts the idle clock,
// reporting ErrNoTicket or ErrTicketExpired when they may not.
//
// The touch is a write on the send path, which is affordable because sends are
// already rate-limited to twenty per thirty seconds per pod -- unlike the inbox
// poll, which runs four times a second and is read-first for exactly that
// reason.
//
// An expired ticket is deleted here rather than left to be found again. A
// record that has already answered its last question is a record that will be
// read wrongly later.
func (s *Store) UseTicket(a, b string) (Ticket, error) {
	var tk Ticket
	now := s.now().UTC()
	key := pairKey(a, b)

	err := s.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(bucketTickets)
		raw := bkt.Get(key)
		if raw == nil {
			return ErrNoTicket
		}
		if err := json.Unmarshal(raw, &tk); err != nil {
			return err
		}
		if tk.expiredAt(now) {
			if err := bkt.Delete(key); err != nil {
				return err
			}
			return ErrTicketExpired
		}
		tk.LastUsed = now
		enc, err := json.Marshal(tk)
		if err != nil {
			return err
		}
		return bkt.Put(key, enc)
	})
	if err != nil {
		return Ticket{}, err
	}
	return tk, nil
}

// LiveTickets lists the tickets that have not gone idle, for the gauge and for
// the operator. Reads only: expiry is applied on use and on the next claim, so
// a scrape never writes.
func (s *Store) LiveTickets() ([]Ticket, error) {
	var out []Ticket
	now := s.now().UTC()
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTickets).
			ForEach(func(_, v []byte) error {
				var tk Ticket
				if err := json.Unmarshal(v, &tk); err != nil {
					return err
				}
				if !tk.expiredAt(now) {
					out = append(out, tk)
				}
				return nil
			})
	})
	return out, err
}

// sweepTickets drops every ticket that has gone idle or that names a channel
// being retired, inside the caller's transaction.
//
// Called from ClaimChannel, which is the one moment starling is certain a
// session has ended. That is also what bounds the bucket: tickets accumulate
// only between claims, and a pair whose ends both went quiet is cleared the
// next time either pod starts a session.
//
// Nothing sweeps while the fleet is perfectly idle, which is a handful of small
// records and is the correct amount of machinery for it.
func sweepTickets(tx *bolt.Tx, retired string, now time.Time) error {
	bkt := tx.Bucket(bucketTickets)
	var dead [][]byte
	err := bkt.ForEach(func(k, v []byte) error {
		var tk Ticket
		if err := json.Unmarshal(v, &tk); err != nil {
			// An unreadable ticket is a ticket nothing can honour.
			// Drop it rather than fail a claim over it.
			dead = append(dead, append([]byte(nil), k...))
			return nil
		}
		if tk.expiredAt(now) || (retired != "" && tk.holds(retired)) {
			dead = append(dead, append([]byte(nil), k...))
		}
		return nil
	})
	if err != nil {
		return err
	}
	for _, k := range dead {
		if err := bkt.Delete(k); err != nil {
			return err
		}
	}
	return nil
}
