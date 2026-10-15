package trafficlogger

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type blockedOnlineWriter struct {
	header  http.Header
	entered chan struct{}
	release chan struct{}
	body    []byte
}

func (w *blockedOnlineWriter) Header() http.Header { return w.header }
func (w *blockedOnlineWriter) WriteHeader(int)     {}
func (w *blockedOnlineWriter) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	w.body = append(w.body, p...)
	return len(p), nil
}

// A stalled management response used to retain the same read lock that
// LogTraffic needs for all users. The response must remain a coherent snapshot
// while unrelated traffic and connection bookkeeping continue.
func TestOnlineSlowReaderDoesNotBlockTraffic(t *testing.T) {
	s := NewTrafficStatsServer("test-secret")
	s.LogOnlineState("first", true)
	w := &blockedOnlineWriter{header: make(http.Header), entered: make(chan struct{}), release: make(chan struct{})}
	req := httptest.NewRequest(http.MethodGet, "/online", nil)
	req.Header.Set("Authorization", "test-secret")
	handlerDone := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(w.release) }) }
	go func() {
		s.ServeHTTP(w, req)
		close(handlerDone)
	}()
	defer func() {
		release()
		<-handlerDone
	}()
	select {
	case <-w.entered:
	case <-time.After(time.Second):
		t.Fatal("online response never reached its write")
	}
	progress := make(chan bool, 1)
	go func() {
		ok := s.LogTraffic("unrelated", 7, 11)
		s.LogOnlineState("second", true)
		progress <- ok
	}()
	select {
	case ok := <-progress:
		if !ok {
			t.Error("unrelated traffic was rejected")
		}
	case <-time.After(time.Second):
		t.Error("slow online response blocked unrelated traffic accounting")
		return
	}
	// The captured bytes are already serialized before the response can block.
	release()
	<-handlerDone
	var snapshot map[string]int
	if err := json.Unmarshal(w.body, &snapshot); err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 1 || snapshot["first"] != 1 {
		t.Fatalf("response changed after snapshot: %v", snapshot)
	}
}
