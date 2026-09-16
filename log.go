package main

// Structured JSON to stdout, one object per line. A bare print is a lint error
// in this estate, and the reason is operational rather than aesthetic: vector
// tails these, and a line that is not JSON is a line that silently does not
// arrive anywhere useful.
//
// Deliberately small. There is no level filtering, no sampling and no
// formatting: starling is four processes exchanging small messages, and a
// logging framework would be more code than the thing it logs for.

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

// Logger writes JSON lines.
type Logger struct {
	mu  sync.Mutex
	out *os.File
	now func() time.Time
}

// NewLogger writes to stdout.
func NewLogger() *Logger {
	return &Logger{out: os.Stdout, now: time.Now}
}

// log writes one record. Fields are copied so a caller reusing its map cannot
// change a line after it was emitted.
func (l *Logger) log(level, msg string, fields map[string]any) {
	rec := map[string]any{
		"ts":      l.now().UTC().Format(time.RFC3339Nano),
		"level":   level,
		"msg":     msg,
		"service": "starling",
	}
	for k, v := range fields {
		rec[k] = v
	}
	// One Encode call under the lock, so two goroutines cannot interleave
	// halves of a line -- which is the failure that makes a log file
	// untrustworthy exactly when it is being read in anger.
	l.mu.Lock()
	defer l.mu.Unlock()
	_ = json.NewEncoder(l.out).Encode(rec)
}

// Info records something that happened.
func (l *Logger) Info(
	msg string,
	fields map[string]any,
) {
	l.log("info", msg, fields)
}

// Warn records something that is wrong but survivable.
func (l *Logger) Warn(
	msg string,
	fields map[string]any,
) {
	l.log("warn", msg, fields)
}

// Error records something that failed.
func (l *Logger) Error(
	msg string,
	fields map[string]any,
) {
	l.log("error", msg, fields)
}
