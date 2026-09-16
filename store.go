package main

// Where messages wait between being accepted and being read.
//
// bbolt, like flipr, for the same reasons: one file, no server, transactional,
// and small enough that its behaviour under failure is knowable. Starling moves
// small JSON objects between four processes; anything more is machinery in
// search of a problem.
//
// What is deliberately NOT here: durability guarantees beyond the file, replay,
// or reconstruction. DESIGN.md states this outright -- if the store is lost,
// every undelivered message is gone, and the remedy is to roll the deployment
// rather than to recover.
//
// That is affordable only because the blast radius is agents and nothing else,
// and because agent work lives in git rather than in here. Do not add a restore
// path; add a reason to not need one.
//
// Channels are the unit, NOT PODS. A pod outlives a session, so mail addressed
// to a pod would be read by whichever session happened to be running when it
// arrived -- including a session that started afterwards and was never part of
// the conversation.
//
// That is the inheritance path session directories already close on the
// filesystem side. Here it is closed by making the channel, not the pod, the
// thing an inbox belongs to.

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	// pod -> current channel. One entry per pod, overwritten on each claim.
	bucketCurrent = []byte("current")
	// channel -> metadata (pod, uid, claimed_at, last_heartbeat)
	bucketChannels = []byte("channels")
	// channel -> nested bucket of seq -> envelope json
	bucketInbox = []byte("inbox")
)

// ErrNoChannel is returned when addressing a pod that has no live session.
// Distinct from "unknown recipient": the difference between nobody by that name
// and nobody home right now is the difference between a typo and a dead agent.
var ErrNoChannel = errors.New("no live channel for that recipient")

// ChannelInfo is what starling knows about one session.
type ChannelInfo struct {
	Channel       string    `json:"channel"`
	Pod           string    `json:"pod"`
	UID           string    `json:"uid"`
	Note          string    `json:"note,omitempty"`
	ClaimedAt     time.Time `json:"claimed_at"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
}

// Store is the message store. Safe for concurrent use; bbolt serialises writes.
type Store struct {
	db  *bolt.DB
	now func() time.Time
}

// OpenStore opens or creates the store at path and ensures its buckets exist.
func OpenStore(path string) (*Store, error) {
	db, err := bolt.Open(
		path,
		0o600,
		&bolt.Options{Timeout: 3 * time.Second},
	)
	if err != nil {
		return nil, fmt.Errorf("opening store at %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		buckets := [][]byte{
			bucketCurrent, bucketChannels,
			bucketInbox, bucketTickets,
		}
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("preparing store buckets: %w", err)
	}
	return &Store{db: db, now: time.Now}, nil
}

// Close releases the store.
func (s *Store) Close() error { return s.db.Close() }

// ClaimChannel opens a channel for a session and makes it the pod's current
// one, deleting the pod's previous channel and everything still in its inbox.
//
// An earlier version left the old channel and its mail in place, "unreachable
// but present", on the theory that keeping it was harmless. It was not.
//
// Nothing ever removed it, so channel records and marooned inboxes grew for the
// life of the store; every /metrics scrape walked all of them; and the file's
// own comment claiming this store "keeps no unbounded history" was false. Mail
// that nothing can read and nothing will delete is a leak with a tidy name.
//
// Deleting it is also the honest reading of the design: a message addressed to
// a session that has ended is undeliverable, and the record of it belongs in
// the log, which is written when it is accepted, rather than in a bucket
// nothing opens.
func (s *Store) ClaimChannel(id Identity, note string) (ChannelInfo, error) {
	info := ChannelInfo{
		Channel:       newChannelID(),
		Pod:           id.Pod,
		UID:           id.UID,
		Note:          note,
		ClaimedAt:     s.now().UTC(),
		LastHeartbeat: s.now().UTC(),
	}
	err := s.db.Update(func(tx *bolt.Tx) error {
		// Retire the previous occupant first, in the same transaction,
		// so a crash between the two cannot leave a pod pointing at a
		// deleted channel.
		retired := ""
		prev := tx.Bucket(bucketCurrent).Get([]byte(id.Pod))
		if prev != nil {
			p := append([]byte(nil), prev...)
			retired = string(p)
			err := tx.Bucket(bucketChannels).Delete(p)
			if err != nil {
				return err
			}
			if inbox := tx.Bucket(bucketInbox); inbox.Bucket(
				p,
			) != nil {
				if err := inbox.DeleteBucket(p); err != nil {
					return err
				}
			}
		}
		// Tickets naming the retired channel go with it: the peer that
		// agreed to talk is gone, and the new session inherits its
		// predecessor's conversations no more than it inherits its
		// mail.
		//
		// This is also the only moment starling is certain a session
		// has ended, so it is where idle tickets get swept as well.
		if err := sweepTickets(tx, retired, s.now().UTC()); err != nil {
			return err
		}
		enc, err := json.Marshal(info)
		if err != nil {
			return err
		}
		err = tx.Bucket(bucketChannels).Put(
			[]byte(info.Channel), enc,
		)
		if err != nil {
			return err
		}
		return tx.Bucket(bucketCurrent).
			Put([]byte(id.Pod), []byte(info.Channel))
	})
	if err != nil {
		return ChannelInfo{}, fmt.Errorf(
			"claiming channel for %s: %w",
			id.Pod,
			err,
		)
	}
	return info, nil
}

// CurrentChannel resolves a pod to its live channel.
func (s *Store) CurrentChannel(pod string) (ChannelInfo, error) {
	var info ChannelInfo
	err := s.db.View(func(tx *bolt.Tx) error {
		ch := tx.Bucket(bucketCurrent).Get([]byte(pod))
		if ch == nil {
			return ErrNoChannel
		}
		raw := tx.Bucket(bucketChannels).Get(ch)
		if raw == nil {
			return ErrNoChannel
		}
		return json.Unmarshal(raw, &info)
	})
	return info, err
}

// Deliver puts an envelope in a channel's inbox. Sequence numbers are bbolt's
// per-bucket counter, so ordering within an inbox is the order of acceptance
// and needs no clock -- two messages accepted in the same millisecond still
// have an order, which a timestamp key would not give.
func (s *Store) Deliver(channel string, env []byte) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if tx.Bucket(bucketChannels).Get([]byte(channel)) == nil {
			return ErrNoChannel
		}
		box, err := tx.Bucket(bucketInbox).
			CreateBucketIfNotExists([]byte(channel))
		if err != nil {
			return err
		}
		seq, err := box.NextSequence()
		if err != nil {
			return err
		}
		key := make([]byte, 8)
		binary.BigEndian.PutUint64(key, seq)
		return box.Put(key, env)
	})
}

// Take removes and returns up to max messages from a channel's inbox.
//
// Removes. A message read is a message delivered, and starling keeps no
// unbounded history: the record that matters for auditing is the counters and
// the log, not a growing pile of every message ever sent.
//
// Redelivery on poller crash is deliberately not provided -- a lost message
// costs one "say that again", and at-least-once delivery would cost duplicate
// keystrokes typed into a live session, which is worse.
func (s *Store) Take(channel string, max int) ([][]byte, error) {
	if max <= 0 {
		max = 32
	}

	// Read first, and this is not a micro-optimisation. Take is called by
	// every poller on every tick of a long-poll -- four times a second per
	// pod while waiting -- and an unconditional db.Update opens a write
	// transaction each time.
	//
	// bbolt serialises writers, so an idle fleet of pollers produced a
	// steady stream of serialised commits doing nothing, and send latency
	// grew with fleet size for no reason anybody could see.
	//
	// An empty inbox is the overwhelmingly common case, so it costs a read.
	empty, err := s.isEmpty(channel)
	if err != nil {
		return nil, err
	}
	if empty {
		return nil, nil
	}

	var out [][]byte
	err = s.db.Update(func(tx *bolt.Tx) error {
		root := tx.Bucket(bucketInbox)
		box := root.Bucket([]byte(channel))
		if box == nil {
			return nil
		}
		c := box.Cursor()
		var keys [][]byte
		for k, v := c.First(); k != nil &&
			len(out) < max; k, v = c.Next() {
			cp := make([]byte, len(v))
			copy(cp, v)
			out = append(out, cp)
			kc := make([]byte, len(k))
			copy(kc, k)
			keys = append(keys, kc)
		}
		for _, k := range keys {
			if err := box.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
	return out, err
}

// isEmpty reports whether a channel's inbox has nothing in it, in a read
// transaction. Racy by nature -- a message can arrive between this and the
// Update below -- and that is harmless: the poll loop simply asks again on its
// next tick, a quarter of a second later.
func (s *Store) isEmpty(channel string) (bool, error) {
	empty := true
	err := s.db.View(func(tx *bolt.Tx) error {
		box := tx.Bucket(bucketInbox).Bucket([]byte(channel))
		if box == nil {
			return nil
		}
		k, _ := box.Cursor().First()
		empty = k == nil
		return nil
	})
	return empty, err
}

// Depth reports how many messages are waiting in a channel's inbox. This is a
// metric rather than a convenience: an agent that has stopped reading is as
// interesting as one that will not stop writing, and depth is how that shows
// up before anybody notices the silence.
func (s *Store) Depth(channel string) (int, error) {
	var n int
	err := s.db.View(func(tx *bolt.Tx) error {
		box := tx.Bucket(bucketInbox).Bucket([]byte(channel))
		if box == nil {
			return nil
		}
		n = box.Stats().KeyN
		return nil
	})
	return n, err
}

// Heartbeat records that a poller is alive. Written by the poller, never by the
// agent: an agent that has stopped reading its input cannot report anything,
// and its silence is otherwise identical to having nothing to say.
func (s *Store) Heartbeat(channel string) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketChannels)
		raw := b.Get([]byte(channel))
		if raw == nil {
			return ErrNoChannel
		}
		var info ChannelInfo
		if err := json.Unmarshal(raw, &info); err != nil {
			return err
		}
		info.LastHeartbeat = s.now().UTC()
		enc, err := json.Marshal(info)
		if err != nil {
			return err
		}
		return b.Put([]byte(channel), enc)
	})
}

// LiveChannels lists every channel, for metrics and for the operator. Includes
// channels whose poller has gone quiet: the point of listing them is to see
// which ones have.
func (s *Store) LiveChannels() ([]ChannelInfo, error) {
	var out []ChannelInfo
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketChannels).
			ForEach(func(_, v []byte) error {
				var info ChannelInfo
				if err := json.Unmarshal(v, &info); err != nil {
					return err
				}
				out = append(out, info)
				return nil
			})
	})
	return out, err
}
