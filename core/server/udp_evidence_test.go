package server

import (
	"errors"
	"github.com/apernet/hysteria/core/v2/internal/frag"
	"github.com/apernet/hysteria/core/v2/internal/protocol"
	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/logging"
	"sync"
	"testing"
	"time"
)

func newTestUDP(t *testing.T) (*quicEvidence, *udpEvidence) {
	t.Helper()
	m, err := newQUICEvidence(QUICEvidenceConfig{Enabled: true, UDPEvidence: true}, evidenceLoggerFunc(func(QUICEvidence) {}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := evidenceContext(1)
	m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
	if m.traceUDP(ctx) != nil {
		t.Fatal("pre-auth UDP observation")
	}
	m.authenticate(ctx, 1, 0)
	<-m.events
	return m, m.traceUDP(ctx)
}
func TestUDPEvidenceBoundedWaitAndCompletion(t *testing.T) {
	m, u := newTestUDP(t)
	var ops []*udpOperation
	for i := 0; i < 8; i++ {
		ops = append(ops, u.begin(udpPhaseSendDatagram, 1200))
	}
	if u.begin(udpPhaseInit, 5) != nil || u.untrackedOperations.Load() != 1 {
		t.Fatal("operation cap not applied")
	}
	s := u.snapshot(time.Now())
	if len(s.Waits) != 8 || s.Waits[0].Direction != "to_client" || s.ReceiveQueueDropsObservable {
		t.Fatal("bad coverage/wait")
	}
	// Simulated monotonic age avoids sleeping merely to test the threshold.
	ops[0].started.Store(time.Since(u.connection.created).Nanoseconds() - int64(1200*time.Millisecond))
	ops[0].complete(1200, nil)
	e := <-m.events
	if e.Event != "udp_operation_slow_complete" || e.UDPOperation.Phase != "send_datagram" || e.UDPOperation.WrittenBytes != 1200 || e.UDPOperation.DurationUS < 1200000 {
		t.Fatalf("bad completion %+v", e)
	}
	for _, op := range ops[1:] {
		op.complete(0, errors.New("synthetic private error"))
	}
	if len(u.snapshot(time.Now()).Waits) != 0 || len(m.events) != 0 {
		t.Fatal("normal completion retained slot/emitted log")
	}
}
func TestUDPEvidenceConcurrentClose(t *testing.T) {
	m, u := newTestUDP(t)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				op := u.begin(udpPhaseSendDatagram, 50)
				_ = u.snapshot(time.Now())
				op.complete(50, nil)
			}
		}()
	}
	m.shutdown()
	wg.Wait()
	if len(u.snapshot(time.Now()).Waits) != 0 {
		t.Fatal("slots leaked")
	}
}
func TestUDPEvidenceActualWriteAndIncompleteFragment(t *testing.T) {
	_, u := newTestUDP(t)
	conn := newMockUDPConn(t)
	conn.EXPECT().WriteTo([]byte("hello"), "synthetic:1").Return(2, errors.New("write failed")).Once()
	e := &udpSessionEntry{D: &frag.Defragger{}, Last: nil, conn: conn, Evidence: u}
	// Use normal constructor for its AtomicTime, without initializing/dialing.
	e = newUDPSessionEntry(1, nil, nil, nil)
	e.conn = conn
	e.Evidence = u
	n, err := e.Feed(&protocol.UDPMessage{FragCount: 1, Addr: "synthetic:1", Data: []byte("hello")})
	if n != 2 || err == nil || u.targetWrittenBytes.Load() != 2 || u.targetWriteErrors.Load() != 1 {
		t.Fatal("actual write/return changed")
	}
	n, err = e.Feed(&protocol.UDPMessage{FragCount: 2, FragID: 0, PacketID: 1, Addr: "synthetic:1", Data: []byte("h")})
	if n != 0 || err != nil || u.defragPending.Load() != 1 || u.targetWriteCalls.Load() != 1 {
		t.Fatal("fragment wait classified as write/drop")
	}
}
func TestUDPEvidenceSerializeDropPreservesReturn(t *testing.T) {
	_, u := newTestUDP(t)
	io := &udpIOImpl{Evidence: u}
	err := io.SendMessage(make([]byte, 1), &protocol.UDPMessage{FragCount: 1, Addr: "synthetic:1", Data: []byte("payload")})
	if err != nil || u.serializeDrops.Load() != 1 || u.sendAttempts.Load() != 1 || u.enqueuedDatagrams.Load() != 0 {
		t.Fatal("silent serialization drop accounting/semantics changed")
	}
}
