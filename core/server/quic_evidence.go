package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/logging"
)

// QUICEvidenceConfig enables aggregate diagnostics without changing transport behavior.
// A slot includes an unauthenticated handshake. Hitting the cap only skips tracing.
type QUICEvidenceConfig struct {
	Enabled        bool
	Interval       time.Duration
	MaxConnections int
}

// QUICEvidenceLogger is called by one background worker, never by a packet callback.
// A blocked logger can delay evidence, but cannot block the transport. The bounded
// event queue drops records instead of creating additional workers or buffering forever.
type QUICEvidenceLogger interface {
	LogQUICEvidence(QUICEvidence)
}

// QUICEvidence contains no addresses, targets, credentials, authentication IDs,
// QUIC connection IDs, packet numbers, payloads, or arbitrary error strings.
// Counters start at authentication. Gauges are independently sampled atomics;
// they do not form a transactionally consistent transport-state snapshot.
type QUICEvidence struct {
	Schema                    int       `json:"schema"`
	Event                     string    `json:"event"`
	Time                      time.Time `json:"time"`
	Session                   string    `json:"session"`
	StatsConnection           uint32    `json:"stats_connection"`
	AuthenticatedAgeMS        int64     `json:"authenticated_age_ms"`
	ReceivedPackets           uint64    `json:"received_packets"`
	ReceivedBytes             uint64    `json:"received_bytes"`
	SentPackets               uint64    `json:"sent_packets"`
	SentBytes                 uint64    `json:"sent_bytes"`
	AcknowledgedPackets       uint64    `json:"acknowledged_packets"`
	LostPackets               uint64    `json:"lost_packets"`
	TimeThresholdLosses       uint64    `json:"time_threshold_losses"`
	ReorderingThresholdLosses uint64    `json:"reordering_threshold_losses"`
	PTOCount                  uint32    `json:"pto_count"`
	PTOMax                    uint32    `json:"pto_max"`
	PTOPositiveUpdates        uint64    `json:"pto_positive_updates"`
	LatestRTTUS               int64     `json:"latest_rtt_us"`
	SmoothedRTTUS             int64     `json:"smoothed_rtt_us"`
	MinRTTUS                  int64     `json:"min_rtt_us"`
	CongestionWindow          int64     `json:"congestion_window_bytes"`
	BytesInFlight             int64     `json:"bytes_in_flight"`
	PacketsInFlight           int64     `json:"packets_in_flight"`
	MTU                       int64     `json:"mtu"`
	LastReceivedAgeMS         int64     `json:"last_received_age_ms"`
	LastSentAgeMS             int64     `json:"last_sent_age_ms"`
	LastACKAgeMS              int64     `json:"last_ack_age_ms"`
	LastMetricsAgeMS          int64     `json:"last_metrics_age_ms"`
	CloseClass                string    `json:"close_class,omitempty"`
	CloseCode                 uint64    `json:"close_code,omitempty"`
	CloseRemote               bool      `json:"close_remote,omitempty"`
	DroppedEvents             uint64    `json:"dropped_events_total"`
	UntrackedConnections      uint64    `json:"untracked_connections_total"`
}

const evidenceQueueCapacity = 64
const evidenceRecordsPerSecond = 64

type evidenceClose struct {
	class  string
	code   uint64
	remote bool
}

type evidenceState struct {
	created       time.Time
	authenticated atomic.Bool
	// Written before authenticated.Store(true), then immutable.
	authAt             time.Time
	session            string
	statsConnection    uint32
	receivedPackets    atomic.Uint64
	receivedBytes      atomic.Uint64
	sentPackets        atomic.Uint64
	sentBytes          atomic.Uint64
	acked              atomic.Uint64
	lost               atomic.Uint64
	lostTime           atomic.Uint64
	lostReorder        atomic.Uint64
	pto                atomic.Uint32
	ptoMax             atomic.Uint32
	ptoPositiveUpdates atomic.Uint64
	latestRTT          atomic.Int64
	smoothedRTT        atomic.Int64
	minRTT             atomic.Int64
	cwnd               atomic.Int64
	inFlight           atomic.Int64
	packetsInFlight    atomic.Int64
	mtu                atomic.Int64
	lastReceived       atomic.Int64
	lastSent           atomic.Int64
	lastACK            atomic.Int64
	lastMetrics        atomic.Int64
	close              atomic.Pointer[evidenceClose]
	ended              atomic.Bool
}

type quicEvidence struct {
	config    QUICEvidenceConfig
	logger    QUICEvidenceLogger
	prefix    string
	mu        sync.Mutex // Connection lifecycle only; packet callbacks never take it.
	states    map[quic.ConnectionTracingID]*evidenceState
	events    chan QUICEvidence
	stop      chan struct{}
	done      chan struct{}
	stopOnce  sync.Once
	dropped   atomic.Uint64
	untracked atomic.Uint64
	// Used only by the single emitter worker.
	rateTime   time.Time
	rateTokens float64
}

func (c *QUICEvidenceConfig) validate(logger QUICEvidenceLogger) error {
	if !c.Enabled {
		return nil
	}
	if logger == nil {
		return errors.New("quicEvidence requires a logger")
	}
	if c.Interval == 0 {
		c.Interval = 5 * time.Second
	}
	if c.Interval < time.Second || c.Interval > time.Minute {
		return errors.New("quicEvidence interval must be between 1s and 1m")
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 128
	}
	if c.MaxConnections < 1 || c.MaxConnections > 512 {
		return errors.New("quicEvidence maxConnections must be between 1 and 512")
	}
	return nil
}

func newQUICEvidence(config QUICEvidenceConfig, logger QUICEvidenceLogger) (*quicEvidence, error) {
	if !config.Enabled {
		return nil, nil
	}
	if err := config.validate(logger); err != nil {
		return nil, err
	}
	var prefix [16]byte
	if _, err := rand.Read(prefix[:]); err != nil {
		return nil, fmt.Errorf("initialize evidence session IDs: %w", err)
	}
	return &quicEvidence{config: config, logger: logger, prefix: hex.EncodeToString(prefix[:]),
		states: make(map[quic.ConnectionTracingID]*evidenceState), events: make(chan QUICEvidence, evidenceQueueCapacity),
		stop: make(chan struct{}), done: make(chan struct{}),
		rateTime: time.Now(), rateTokens: float64(config.MaxConnections + evidenceQueueCapacity)}, nil
}

func (m *quicEvidence) tracer(ctx context.Context, _ logging.Perspective, _ quic.ConnectionID) *logging.ConnectionTracer {
	id, ok := ctx.Value(quic.ConnectionTracingKey).(quic.ConnectionTracingID)
	if !ok {
		m.untracked.Add(1)
		return nil
	}
	m.mu.Lock()
	if len(m.states) >= m.config.MaxConnections {
		m.mu.Unlock()
		m.untracked.Add(1)
		return nil
	}
	s := &evidenceState{created: time.Now()}
	s.lastReceived.Store(-1)
	s.lastSent.Store(-1)
	s.lastACK.Store(-1)
	s.lastMetrics.Store(-1)
	m.states[id] = s
	m.mu.Unlock()
	return &logging.ConnectionTracer{
		ReceivedLongHeaderPacket: func(_ *logging.ExtendedHeader, size logging.ByteCount, _ logging.ECN, _ []logging.Frame) {
			s.received(size)
		},
		ReceivedShortHeaderPacket: func(_ *logging.ShortHeader, size logging.ByteCount, _ logging.ECN, _ []logging.Frame) {
			s.received(size)
		},
		SentLongHeaderPacket: func(_ *logging.ExtendedHeader, size logging.ByteCount, _ logging.ECN, _ *logging.AckFrame, _ []logging.Frame) {
			s.sent(size)
		},
		SentShortHeaderPacket: func(_ *logging.ShortHeader, size logging.ByteCount, _ logging.ECN, _ *logging.AckFrame, _ []logging.Frame) {
			s.sent(size)
		},
		AcknowledgedPacket: func(_ logging.EncryptionLevel, _ logging.PacketNumber) {
			if !s.authenticated.Load() {
				return
			}
			s.acked.Add(1)
			s.lastACK.Store(time.Since(s.created).Nanoseconds())
		},
		LostPacket: func(_ logging.EncryptionLevel, _ logging.PacketNumber, reason logging.PacketLossReason) {
			if !s.authenticated.Load() {
				return
			}
			s.lost.Add(1)
			switch reason {
			case logging.PacketLossTimeThreshold:
				s.lostTime.Add(1)
			case logging.PacketLossReorderingThreshold:
				s.lostReorder.Add(1)
			}
		},
		UpdatedPTOCount: func(n uint32) {
			if !s.authenticated.Load() {
				return
			}
			s.pto.Store(n)
			if n > 0 {
				s.ptoPositiveUpdates.Add(1)
			}
			for old := s.ptoMax.Load(); n > old && !s.ptoMax.CompareAndSwap(old, n); old = s.ptoMax.Load() {
			}
		},
		UpdatedMetrics: func(rtt *logging.RTTStats, cwnd, inFlight logging.ByteCount, packets int) {
			if !s.authenticated.Load() {
				return
			}
			// Do not retain the transport's mutable RTTStats pointer.
			s.latestRTT.Store(rtt.LatestRTT().Microseconds())
			s.smoothedRTT.Store(rtt.SmoothedRTT().Microseconds())
			s.minRTT.Store(rtt.MinRTT().Microseconds())
			s.cwnd.Store(int64(cwnd))
			s.inFlight.Store(int64(inFlight))
			s.packetsInFlight.Store(int64(packets))
			s.lastMetrics.Store(time.Since(s.created).Nanoseconds())
		},
		UpdatedMTU: func(mtu logging.ByteCount, _ bool) {
			if s.authenticated.Load() {
				s.mtu.Store(int64(mtu))
			}
		},
		ClosedConnection: func(err error) { s.close.Store(classifyQUICClose(err)) },
		Close: func() {
			m.mu.Lock()
			if s.ended.Swap(true) {
				m.mu.Unlock()
				return
			}
			delete(m.states, id)
			m.mu.Unlock()
			if s.authenticated.Load() {
				m.enqueue(s.snapshot("closed", time.Now()))
			}
		},
	}
}

func (s *evidenceState) received(size logging.ByteCount) {
	if !s.authenticated.Load() {
		return
	}
	s.receivedPackets.Add(1)
	if size > 0 {
		s.receivedBytes.Add(uint64(size))
	}
	s.lastReceived.Store(time.Since(s.created).Nanoseconds())
}

func (s *evidenceState) sent(size logging.ByteCount) {
	if !s.authenticated.Load() {
		return
	}
	s.sentPackets.Add(1)
	if size > 0 {
		s.sentBytes.Add(uint64(size))
	}
	s.lastSent.Store(time.Since(s.created).Nanoseconds())
}

func (m *quicEvidence) authenticate(ctx context.Context, statsConnection uint32) {
	if m == nil {
		return
	}
	id, ok := ctx.Value(quic.ConnectionTracingKey).(quic.ConnectionTracingID)
	if !ok {
		return
	}
	m.mu.Lock()
	s := m.states[id]
	if s == nil || s.authenticated.Load() {
		m.mu.Unlock()
		return
	}
	s.authAt = time.Now()
	s.session = fmt.Sprintf("%s-%x", m.prefix, uint64(id))
	s.statsConnection = statsConnection
	s.authenticated.Store(true)
	m.mu.Unlock()
	m.enqueue(s.snapshot("authenticated", time.Now()))
}

func evidenceAge(now time.Time, created time.Time, ns int64) int64 {
	if ns < 0 {
		return -1
	}
	return max(0, (now.Sub(created).Nanoseconds()-ns)/int64(time.Millisecond))
}

func (s *evidenceState) snapshot(event string, now time.Time) QUICEvidence {
	e := QUICEvidence{Schema: 1, Event: event, Time: now.UTC(), Session: s.session, StatsConnection: s.statsConnection,
		AuthenticatedAgeMS: max(0, now.Sub(s.authAt).Milliseconds()),
		ReceivedPackets:    s.receivedPackets.Load(), ReceivedBytes: s.receivedBytes.Load(), SentPackets: s.sentPackets.Load(), SentBytes: s.sentBytes.Load(),
		AcknowledgedPackets: s.acked.Load(), LostPackets: s.lost.Load(), TimeThresholdLosses: s.lostTime.Load(), ReorderingThresholdLosses: s.lostReorder.Load(),
		PTOCount: s.pto.Load(), PTOMax: s.ptoMax.Load(), PTOPositiveUpdates: s.ptoPositiveUpdates.Load(),
		LatestRTTUS: s.latestRTT.Load(), SmoothedRTTUS: s.smoothedRTT.Load(), MinRTTUS: s.minRTT.Load(),
		CongestionWindow: s.cwnd.Load(), BytesInFlight: s.inFlight.Load(), PacketsInFlight: s.packetsInFlight.Load(), MTU: s.mtu.Load(),
		LastReceivedAgeMS: evidenceAge(now, s.created, s.lastReceived.Load()), LastSentAgeMS: evidenceAge(now, s.created, s.lastSent.Load()),
		LastACKAgeMS: evidenceAge(now, s.created, s.lastACK.Load()), LastMetricsAgeMS: evidenceAge(now, s.created, s.lastMetrics.Load())}
	if close := s.close.Load(); close != nil {
		e.CloseClass = close.class
		e.CloseCode = close.code
		e.CloseRemote = close.remote
	}
	return e
}

func (m *quicEvidence) enqueue(e QUICEvidence) {
	select {
	case m.events <- e:
	default:
		m.dropped.Add(1)
	}
}

func (m *quicEvidence) emit(e QUICEvidence) {
	if !m.allowRecord(time.Now()) {
		m.dropped.Add(1)
		return
	}
	e.DroppedEvents = m.dropped.Load()
	e.UntrackedConnections = m.untracked.Load()
	m.logger.LogQUICEvidence(e)
}

// Permit a bounded snapshot batch without losing half the default 128-slot
// registry to a fixed per-second cutoff. Sustained output remains rate limited.
func (m *quicEvidence) allowRecord(now time.Time) bool {
	elapsed := max(0, now.Sub(m.rateTime).Seconds())
	m.rateTime = now
	m.rateTokens = min(float64(m.config.MaxConnections+evidenceQueueCapacity), m.rateTokens+elapsed*evidenceRecordsPerSecond)
	if m.rateTokens < 1 {
		return false
	}
	m.rateTokens--
	return true
}

func (m *quicEvidence) run() {
	defer close(m.done)
	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stop:
			return
		case event := <-m.events:
			m.emit(event)
		case <-ticker.C:
			m.mu.Lock()
			states := make([]*evidenceState, 0, len(m.states))
			for _, s := range m.states {
				if s.authenticated.Load() {
					states = append(states, s)
				}
			}
			m.mu.Unlock()
			for _, s := range states {
				select {
				case <-m.stop:
					return
				default:
				}
				if !s.ended.Load() {
					m.emit(s.snapshot("sample", time.Now()))
				}
			}
		}
	}
}

// Shutdown never waits for a potentially blocked log destination. At most one
// emitter per server instance remains blocked until that destination resumes.
// Repeatedly recreating servers against a permanently blocked sink can retain
// one worker per instance; the logger interface has no cancellation primitive.
func (m *quicEvidence) shutdown() {
	if m != nil {
		m.stopOnce.Do(func() { close(m.stop) })
	}
}

func classifyQUICClose(err error) *evidenceClose {
	if err == nil {
		return &evidenceClose{class: "closed"}
	}
	var idle *quic.IdleTimeoutError
	var handshake *quic.HandshakeTimeoutError
	var application *quic.ApplicationError
	var transport *quic.TransportError
	var reset *quic.StatelessResetError
	switch {
	case errors.As(err, &idle):
		return &evidenceClose{class: "idle_timeout"}
	case errors.As(err, &handshake):
		return &evidenceClose{class: "handshake_timeout"}
	case errors.As(err, &application):
		return &evidenceClose{class: "application", code: uint64(application.ErrorCode), remote: application.Remote}
	case errors.As(err, &transport):
		return &evidenceClose{class: "transport", code: uint64(transport.ErrorCode), remote: transport.Remote}
	case errors.As(err, &reset):
		return &evidenceClose{class: "stateless_reset"}
	default:
		return &evidenceClose{class: "other"}
	}
}
