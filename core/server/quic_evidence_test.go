package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/logging"
)

type evidenceLoggerFunc func(QUICEvidence)

func (f evidenceLoggerFunc) LogQUICEvidence(e QUICEvidence) { f(e) }

func evidenceContext(id uint64) context.Context {
	return context.WithValue(context.Background(), quic.ConnectionTracingKey, quic.ConnectionTracingID(id))
}

func testEvidence(t testing.TB, maxConnections int, logger QUICEvidenceLogger) *quicEvidence {
	t.Helper()
	if logger == nil {
		logger = evidenceLoggerFunc(func(QUICEvidence) {})
	}
	m, err := newQUICEvidence(QUICEvidenceConfig{Enabled: true, MaxConnections: maxConnections}, logger)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestQUICEvidenceDisabledAndBounds(t *testing.T) {
	m, err := newQUICEvidence(QUICEvidenceConfig{}, nil)
	if m != nil || err != nil {
		t.Fatalf("disabled evidence changed behavior: %v, %v", m, err)
	}
	for _, cfg := range []QUICEvidenceConfig{
		{Enabled: true, Interval: time.Millisecond},
		{Enabled: true, Interval: 2 * time.Minute},
		{Enabled: true, MaxConnections: 513},
		{Enabled: true, MaxConnections: -1},
	} {
		if _, err := newQUICEvidence(cfg, evidenceLoggerFunc(func(QUICEvidence) {})); err == nil {
			t.Fatalf("accepted invalid config: %+v", cfg)
		}
	}
	if _, err := newQUICEvidence(QUICEvidenceConfig{Enabled: true}, nil); err == nil {
		t.Fatal("accepted missing logger")
	}
	m = testEvidence(t, 0, nil)
	if m.config.Interval != 5*time.Second || m.config.MaxConnections != 128 {
		t.Fatalf("unexpected defaults: %+v", m.config)
	}
}

func TestQUICEvidenceAuthenticationGateAndPrivacy(t *testing.T) {
	m := testEvidence(t, 2, nil)
	ctx := evidenceContext(1)
	tracer := m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
	tracer.ReceivedShortHeaderPacket(nil, 1234, logging.ECTNot, nil)
	tracer.SentShortHeaderPacket(nil, 4567, logging.ECTNot, nil, nil)
	tracer.AcknowledgedPacket(logging.Encryption1RTT, 9)
	tracer.UpdatedPTOCount(1)
	tracer.ClosedConnection(errors.New("private-address credential target"))
	tracer.Close()
	if len(m.events) != 0 || len(m.states) != 0 {
		t.Fatal("unauthenticated connection emitted evidence or retained state")
	}

	ctx = evidenceContext(2)
	tracer = m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
	preAuthRTT := &logging.RTTStats{}
	preAuthRTT.UpdateRTT(50*time.Millisecond, 0)
	tracer.UpdatedMetrics(preAuthRTT, 50000, 1000, 1)
	tracer.UpdatedMTU(1400, true)
	m.authenticate(ctx, 77)
	m.authenticate(ctx, 88) // A repeated auth request must not change the session.
	first := <-m.events
	if first.Event != "authenticated" || first.StatsConnection != 77 || first.Session == "" || len(m.events) != 0 {
		t.Fatalf("bad auth event: %+v", first)
	}
	if first.LastReceivedAgeMS != -1 || first.ReceivedPackets != 0 {
		t.Fatal("missing observation must not look like fresh progress")
	}
	if first.LastMetricsAgeMS != -1 || first.LatestRTTUS != 0 || first.CongestionWindow != 0 || first.MTU != 0 {
		t.Fatal("pre-auth metrics must remain unknown until an authenticated callback supplies them")
	}
	tracer.ClosedConnection(&quic.ApplicationError{Remote: true, ErrorCode: 0x105, ErrorMessage: "private-address credential target"})
	tracer.Close()
	last := <-m.events
	if last.Session != first.Session || last.CloseClass != "application" || last.CloseCode != 0x105 || !last.CloseRemote {
		t.Fatalf("bad close event: %+v", last)
	}
	encoded, err := json.Marshal(last)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private-address", "credential", "target", "ErrorMessage"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("private value exported: %s", secret)
		}
	}
}

func TestQUICEvidenceMetricsAndClosure(t *testing.T) {
	m := testEvidence(t, 1, nil)
	ctx := evidenceContext(3)
	tracer := m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
	m.authenticate(ctx, 8)
	<-m.events
	tracer.ReceivedShortHeaderPacket(nil, 1200, logging.ECTNot, nil)
	tracer.ReceivedLongHeaderPacket(nil, 100, logging.ECTNot, nil)
	tracer.SentShortHeaderPacket(nil, 1250, logging.ECTNot, nil, nil)
	tracer.AcknowledgedPacket(logging.Encryption1RTT, 1)
	tracer.LostPacket(logging.Encryption1RTT, 2, logging.PacketLossTimeThreshold)
	tracer.LostPacket(logging.Encryption1RTT, 3, logging.PacketLossReorderingThreshold)
	tracer.UpdatedPTOCount(1)
	tracer.UpdatedPTOCount(2)
	tracer.UpdatedPTOCount(0)
	rtt := &logging.RTTStats{}
	rtt.UpdateRTT(40*time.Millisecond, 0)
	tracer.UpdatedMetrics(rtt, 65536, 2500, 2)
	tracer.UpdatedMTU(1400, true)
	tracer.ClosedConnection(&quic.IdleTimeoutError{})
	tracer.Close()
	e := <-m.events
	if e.ReceivedPackets != 2 || e.ReceivedBytes != 1300 || e.SentPackets != 1 || e.SentBytes != 1250 {
		t.Fatalf("bad packet counters: %+v", e)
	}
	if e.AcknowledgedPackets != 1 || e.LostPackets != 2 || e.TimeThresholdLosses != 1 || e.ReorderingThresholdLosses != 1 {
		t.Fatalf("bad ACK/loss counters: %+v", e)
	}
	if e.PTOCount != 0 || e.PTOMax != 2 || e.PTOPositiveUpdates != 2 {
		t.Fatalf("PTO reset lost historical spike: %+v", e)
	}
	if e.LatestRTTUS != 40000 || e.SmoothedRTTUS != 40000 || e.MinRTTUS != 40000 || e.CongestionWindow != 65536 || e.BytesInFlight != 2500 || e.PacketsInFlight != 2 || e.MTU != 1400 {
		t.Fatalf("bad gauges: %+v", e)
	}
	if e.CloseClass != "idle_timeout" || e.LastReceivedAgeMS < 0 || e.LastACKAgeMS < 0 || e.LastMetricsAgeMS < 0 {
		t.Fatalf("bad freshness/close: %+v", e)
	}
	if len(m.states) != 0 {
		t.Fatal("closed connection retained its registry slot")
	}
}

func TestQUICEvidenceBoundedSlotsAndQueue(t *testing.T) {
	m := testEvidence(t, 1, nil)
	first := m.tracer(evidenceContext(1), logging.PerspectiveServer, quic.ConnectionID{})
	if m.tracer(evidenceContext(2), logging.PerspectiveServer, quic.ConnectionID{}) != nil {
		t.Fatal("tracer slot cap exceeded")
	}
	first.Close()
	if m.untracked.Load() != 1 {
		t.Fatal("untracked connection was not counted")
	}
	for id := uint64(3); id < 203; id++ {
		ctx := evidenceContext(id)
		tracer := m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
		if tracer == nil {
			t.Fatal("released slot not reusable")
		}
		m.authenticate(ctx, uint32(id))
		tracer.ClosedConnection(nil)
		tracer.Close()
	}
	if len(m.events) != evidenceQueueCapacity || m.dropped.Load() == 0 || len(m.states) != 0 {
		t.Fatal("event queue or registry is not bounded")
	}
}

func TestQUICEvidenceBlockedLoggerDoesNotBlockTransport(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	m := testEvidence(t, 1, evidenceLoggerFunc(func(QUICEvidence) {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
	}))
	go m.run()
	ctx := evidenceContext(1)
	tracer := m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
	m.authenticate(ctx, 1)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("logger did not run")
	}
	finished := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			tracer.ReceivedShortHeaderPacket(nil, 1200, logging.ECTNot, nil)
		}
		tracer.ClosedConnection(&quic.IdleTimeoutError{})
		tracer.Close()
		m.shutdown()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("logging blocked transport/shutdown")
	}
	close(release)
	select {
	case <-m.done:
	case <-time.After(time.Second):
		t.Fatal("worker did not exit after logger resumed")
	}
}

func TestQUICEvidenceConcurrentSnapshot(t *testing.T) {
	m := testEvidence(t, 1, nil)
	ctx := evidenceContext(1)
	tracer := m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
	m.authenticate(ctx, 1)
	s := m.states[1]
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for n := 0; n < 1000; n++ {
				tracer.ReceivedShortHeaderPacket(nil, 1200, logging.ECTNot, nil)
				tracer.AcknowledgedPacket(logging.Encryption1RTT, 1)
				_ = s.snapshot("sample", time.Now())
			}
		}()
	}
	workers.Wait()
	if s.receivedPackets.Load() != 4000 || s.acked.Load() != 4000 {
		t.Fatal("concurrent counters lost updates")
	}
	tracer.ClosedConnection(nil)
	tracer.Close()
}

func TestQUICEvidenceBlockedSinkBoundIsPerServerInstance(t *testing.T) {
	release := make(chan struct{})
	entered := make(chan struct{}, 3)
	var instances []*quicEvidence
	for i := 0; i < 3; i++ {
		m := testEvidence(t, 1, evidenceLoggerFunc(func(QUICEvidence) { entered <- struct{}{}; <-release }))
		instances = append(instances, m)
		go m.run()
		ctx := evidenceContext(1)
		m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
		m.authenticate(ctx, 1)
		select {
		case <-entered:
		case <-time.After(time.Second):
			close(release)
			t.Fatal("emitter did not reach test sink")
		}
		m.shutdown()
		select {
		case <-m.done:
			close(release)
			t.Fatal("blocked sink unexpectedly became cancellable")
		default:
		}
	}
	// A logger has no cancellation API: shutdown can leave one blocked worker
	// per old instance. Confirm all are released once the sink resumes.
	close(release)
	for _, m := range instances {
		select {
		case <-m.done:
		case <-time.After(time.Second):
			t.Fatal("old instance worker did not exit")
		}
	}
}

func TestQUICEvidenceRateLimit(t *testing.T) {
	m := testEvidence(t, 128, nil)
	now := time.Now()
	m.rateTime = now
	burst := m.config.MaxConnections + evidenceQueueCapacity
	for i := 0; i < burst; i++ {
		if !m.allowRecord(now) {
			t.Fatal("normal snapshot burst rejected")
		}
	}
	if m.allowRecord(now) {
		t.Fatal("record burst is unbounded")
	}
	for i := 0; i < evidenceRecordsPerSecond; i++ {
		if !m.allowRecord(now.Add(time.Second)) {
			t.Fatal("rate budget did not refill")
		}
	}
	if m.allowRecord(now.Add(time.Second)) {
		t.Fatal("sustained rate is unbounded")
	}
}

func BenchmarkQUICEvidenceCallbacks(b *testing.B) {
	for _, authenticated := range []bool{false, true} {
		name := "before_auth"
		if authenticated {
			name = "authenticated"
		}
		b.Run(name, func(b *testing.B) {
			m := testEvidence(b, 1, nil)
			ctx := evidenceContext(1)
			tracer := m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
			if authenticated {
				m.authenticate(ctx, 1)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tracer.ReceivedShortHeaderPacket(nil, 1200, logging.ECTNot, nil)
				tracer.AcknowledgedPacket(logging.Encryption1RTT, 1)
			}
		})
	}
}
