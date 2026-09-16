package main

// Identifiers starling mints.
//
// Random rather than sequential, and from crypto/rand rather than math/rand.
//
// Not to resist guessing -- an agent pod has no inbound surface, so guessing a
// channel buys nothing -- but because sequential ids leak volume and ordering
// into every log line and dashboard label that carries one, and because a
// collision after a restart would silently merge two sessions' mail.
//
// The cost of getting this right is one function.

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// newChannelID returns an id for one session's channel. 16 bytes: collision is
// not a thing that happens, and there is no reason to think about it again.
func newChannelID() string {
	return "ch_" + randomHex(16)
}

// newMessageID returns an id for one envelope. Threading (in_reply_to) points
// at these, so they outlive the message itself in logs and must not be reused.
func newMessageID() string {
	return "msg_" + randomHex(12)
}

// newTicketID returns an id for one pair of sessions' permission to talk. It
// is a handle for logs and for the operator, never a credential: nothing
// accepts it back, and a message carrying one fails to decode.
func newTicketID() string {
	return "tk_" + randomHex(12)
}

// randomHex returns n cryptographically random bytes, hex encoded. Panics if
// the system entropy source fails, which is not a condition to paper over: a
// process that cannot get random bytes cannot mint identifiers safely, and
// continuing with a predictable fallback would be worse than stopping.
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(
			fmt.Sprintf(
				"starling: system entropy unavailable: %v",
				err,
			),
		)
	}
	return hex.EncodeToString(b)
}
