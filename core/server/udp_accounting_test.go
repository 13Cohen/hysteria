package server

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/apernet/hysteria/core/v2/internal/protocol"
	"github.com/apernet/quic-go"
)

// These tests run the real UDP serialization, fragmentation and accounting
// path. The QUIC boundary is in memory: no sockets, sleeps or production traffic.
type accountingTestConn struct {
	send  func([]byte) error
	close func(quic.ApplicationErrorCode, string) error
}

func (c *accountingTestConn) ReceiveDatagram(context.Context) ([]byte, error) {
	panic("unexpected receive")
}
func (c *accountingTestConn) SendDatagram(b []byte) error { return c.send(b) }
func (c *accountingTestConn) CloseWithError(code quic.ApplicationErrorCode, reason string) error {
	return c.close(code, reason)
}

type accountingTestLogger struct {
	log func(string, uint64, uint64) bool
}

func (l *accountingTestLogger) LogTraffic(id string, tx, rx uint64) bool {
	return l.log(id, tx, rx)
}
func (*accountingTestLogger) LogOnlineState(string, bool)        {}
func (*accountingTestLogger) TraceStream(HyStream, *StreamStats) {}
func (*accountingTestLogger) UntraceStream(HyStream)             {}

func accountingTestMessage(data []byte) *protocol.UDPMessage {
	return &protocol.UDPMessage{SessionID: 7, FragCount: 1, Addr: "synthetic.invalid:1", Data: data}
}

func TestUDPPayloadAccounting(t *testing.T) {
	ordinaryError := errors.New("synthetic send error")
	tooLargeAgain := &quic.DatagramTooLargeError{MaxDatagramPayloadSize: 120}
	for _, tc := range []struct {
		name         string
		size         int
		fragment     bool
		bufferSize   int
		failAt       int
		sendError    error
		wantAttempts int
		wantEnqueued int
		wantBytes    uint64
		wantTooLarge uint64
		wantErrors   uint64
		wantDrop     uint64
	}{
		{name: "plain", size: 100, wantAttempts: 1, wantEnqueued: 1, wantBytes: 100},
		{name: "zero_payload", wantAttempts: 1, wantEnqueued: 1},
		{name: "ordinary_failure_still_accounts_attempt", size: 100, failAt: 1, sendError: ordinaryError, wantAttempts: 1, wantErrors: 1},
		{name: "fragmented_once", size: 250, fragment: true, wantAttempts: 4, wantEnqueued: 3, wantBytes: 250, wantTooLarge: 1},
		{name: "first_fragment_fails", size: 250, fragment: true, failAt: 2, sendError: ordinaryError, wantAttempts: 2, wantTooLarge: 1, wantErrors: 1},
		{name: "second_fragment_fails", size: 250, fragment: true, failAt: 3, sendError: ordinaryError, wantAttempts: 3, wantEnqueued: 1, wantBytes: 100, wantTooLarge: 1, wantErrors: 1},
		{name: "fragment_too_large_not_recursively_retried", size: 250, fragment: true, failAt: 2, sendError: tooLargeAgain, wantAttempts: 2, wantTooLarge: 2},
		{name: "serialize_drop_still_accounts_attempt", size: 100, bufferSize: 1, wantAttempts: 1, wantDrop: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, u := newTestUDP(t)
			data := bytes.Repeat([]byte{0x5a}, tc.size)
			msg := accountingTestMessage(data)
			limit := int64(msg.HeaderSize() + 100)
			calls, sends := 0, 0
			var received []byte
			var order []string
			logger := &accountingTestLogger{log: func(id string, tx, rx uint64) bool {
				calls++
				order = append(order, "account")
				if id != "synthetic" || tx != 0 || rx != uint64(tc.size) {
					t.Errorf("accounted (%q,%d,%d), want original payload %d", id, tx, rx, tc.size)
				}
				return true
			}}
			conn := &accountingTestConn{send: func(b []byte) error {
				sends++
				order = append(order, "send")
				if calls != 1 {
					t.Errorf("send requires exactly one preceding original-message authorization: %d", calls)
				}
				if tc.fragment && sends == 1 {
					return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: limit}
				}
				if sends == tc.failAt {
					return tc.sendError
				}
				// Existing SendMessage serializes and sends a zero-byte payload;
				// the receive parser independently rejects empty data. Preserve
				// the send/accounting contract without changing that protocol rule.
				if tc.size == 0 {
					if len(b) != msg.HeaderSize() {
						t.Fatal("zero-payload serialization changed")
					}
					return nil
				}
				parsed, err := protocol.ParseUDPMessage(b)
				if err != nil {
					t.Fatal(err)
				}
				if tc.fragment && (parsed.PacketID == 0 || parsed.FragCount != 3 || int(parsed.FragID) != sends-2) {
					t.Fatalf("fragment header changed: %+v", parsed)
				}
				received = append(received, parsed.Data...)
				return nil
			}, close: func(quic.ApplicationErrorCode, string) error {
				t.Fatal("non-denied accounting must not close QUIC")
				return nil
			}}
			io := &udpIOImpl{Conn: conn, AuthID: "synthetic", TrafficLogger: logger, Evidence: u}
			bufferSize := tc.bufferSize
			if bufferSize == 0 {
				bufferSize = protocol.MaxUDPSize
			}
			err := sendMessageAutoFrag(io, make([]byte, bufferSize), msg)
			if !errors.Is(err, tc.sendError) {
				t.Fatalf("returned %v, want %v", err, tc.sendError)
			}
			if calls != 1 || len(order) == 0 || order[0] != "account" {
				t.Fatalf("account once before any send: calls=%d order=%v", calls, order)
			}
			if u.sendAttempts.Load() != uint64(tc.wantAttempts) || u.enqueuedDatagrams.Load() != uint64(tc.wantEnqueued) || u.enqueuedPayloadBytes.Load() != tc.wantBytes || u.sendTooLarge.Load() != tc.wantTooLarge || u.sendErrors.Load() != tc.wantErrors || u.serializeDrops.Load() != tc.wantDrop {
				t.Fatalf("attempt/outcome evidence changed: %+v", u.snapshot(time.Now()))
			}
			if tc.fragment && (u.fragmentedMessages.Load() != 1 || u.fragmentsAttempted.Load() != uint64(tc.wantAttempts-1)) {
				t.Fatal("fragment attempt evidence changed")
			}
			if tc.sendError == nil && tc.wantDrop == 0 && !bytes.Equal(received, data) {
				t.Fatal("fragmentation changed original payload")
			}
		})
	}
}

func TestUDPPayloadAccountingDeniedBeforeAnySend(t *testing.T) {
	for _, size := range []int{0, 100, 250} {
		_, u := newTestUDP(t)
		var order []string
		conn := &accountingTestConn{send: func([]byte) error {
			t.Fatal("denied message reached SendDatagram")
			return nil
		}, close: func(code quic.ApplicationErrorCode, reason string) error {
			order = append(order, "close")
			if code != closeErrCodeTrafficLimitReached || reason != "" {
				t.Fatalf("close semantics changed: %d %q", code, reason)
			}
			return errors.New("ignored close error")
		}}
		logger := &accountingTestLogger{log: func(_ string, tx, rx uint64) bool {
			order = append(order, "account")
			if tx != 0 || rx != uint64(size) {
				t.Fatalf("wrong denied payload charge: %d/%d", tx, rx)
			}
			return false
		}}
		msg := accountingTestMessage(make([]byte, size))
		io := &udpIOImpl{Conn: conn, TrafficLogger: logger, Evidence: u}
		// A one-byte buffer also proves authorization remains before serialization.
		err := sendMessageAutoFrag(io, make([]byte, 1), msg)
		if err != errDisconnect || !reflect.DeepEqual(order, []string{"account", "close"}) || msg.PacketID != 0 || u.sendAttempts.Load() != 1 || u.serializeDrops.Load() != 0 {
			t.Fatalf("denial changed: err=%v order=%v", err, order)
		}
	}
}

func TestUDPPayloadAccountingConcurrentOriginalMessages(t *testing.T) {
	const count, size = 16, 250
	var mu sync.Mutex
	calls, sends, charged := 0, 0, uint64(0)
	conn := &accountingTestConn{send: func(b []byte) error {
		m, err := protocol.ParseUDPMessage(b)
		if err != nil {
			return err
		}
		mu.Lock()
		sends++
		mu.Unlock()
		if m.FragCount == 1 {
			return &quic.DatagramTooLargeError{MaxDatagramPayloadSize: int64(m.HeaderSize() + 100)}
		}
		return nil
	}, close: func(quic.ApplicationErrorCode, string) error { panic("unexpected close") }}
	logger := &accountingTestLogger{log: func(_ string, _, rx uint64) bool {
		mu.Lock()
		defer mu.Unlock()
		calls++
		charged += rx
		return true
	}}
	io := &udpIOImpl{Conn: conn, TrafficLogger: logger}
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := sendMessageAutoFrag(io, make([]byte, protocol.MaxUDPSize), accountingTestMessage(make([]byte, size))); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if calls != count || charged != count*size || sends != count*4 {
		t.Fatalf("per-original authorization leaked across calls: calls=%d bytes=%d sends=%d", calls, charged, sends)
	}
	// A new original message must not inherit a previous authorization decision.
	io.TrafficLogger = nil
	if err := sendMessageAutoFrag(io, make([]byte, protocol.MaxUDPSize), accountingTestMessage(make([]byte, size))); err != nil {
		t.Fatal(err)
	}
	if calls != count || sends != (count+1)*4 {
		t.Fatal("nil logger must retain fragmentation without accounting")
	}
}
