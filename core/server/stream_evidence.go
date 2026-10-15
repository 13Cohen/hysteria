package server

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"github.com/apernet/quic-go"
)

// Stream evidence observes only the existing TrafficLogger copy loop. It never
// wraps io.Copy, alters its ReaderFrom/WriterTo dispatch, or exports targets.
type StreamDirectionEvidence struct {
	Phase               string `json:"phase"`
	PhaseAgeMS          int64  `json:"phase_age_ms"`
	ReadBytes           uint64 `json:"read_bytes"`
	WrittenBytes        uint64 `json:"written_bytes"`
	PendingBytes        int64  `json:"pending_bytes"`
	MaxCompletedWriteUS int64  `json:"max_completed_write_us"`
	ShortWrites         uint64 `json:"short_writes"`
	Finished            bool   `json:"finished"`
	ErrorClass          string `json:"error_class,omitempty"`
}

type StreamOperationEvidence struct {
	StreamID       uint64    `json:"stream_id"`
	Direction      string    `json:"direction"`
	Phase          string    `json:"phase"`
	StartedAt      time.Time `json:"started_at"`
	DurationUS     int64     `json:"duration_us"`
	RequestedBytes int       `json:"requested_bytes"`
	WrittenBytes   int       `json:"written_bytes"`
	ErrorClass     string    `json:"error_class"`
}

type StreamEvidence struct {
	ID         uint64                  `json:"id"`
	AgeMS      int64                   `json:"age_ms"`
	ToClient   StreamDirectionEvidence `json:"to_client"`
	ToTarget   StreamDirectionEvidence `json:"to_target"`
	BestEffort bool                    `json:"best_effort"`
}

type StreamWaitSummary struct {
	PendingBytes       int64 `json:"pending_bytes"`
	WritingStreams     int   `json:"writing_streams"`
	AccountingStreams  int   `json:"accounting_streams"`
	MaxWriteAgeMS      int64 `json:"max_write_age_ms"`
	MaxAccountingAgeMS int64 `json:"max_accounting_age_ms"`
}

type StreamTerminalDirections struct {
	None          uint64 `json:"none"`
	EOF           uint64 `json:"eof"`
	Closed        uint64 `json:"closed"`
	TrafficDenied uint64 `json:"traffic_denied"`
	Timeout       uint64 `json:"timeout"`
	Other         uint64 `json:"other"`
}
type ApplicationEvidence struct {
	CompletedStreams          uint64                   `json:"completed_streams_total"`
	IncompleteCloses          uint64                   `json:"incomplete_closes_total"`
	SuppressedRoutineCloses   uint64                   `json:"suppressed_routine_closes_total"`
	TerminalDirections        StreamTerminalDirections `json:"terminal_directions"`
	ObservedActiveStreams     int                      `json:"observed_active_streams"`
	ToClient                  StreamWaitSummary        `json:"to_client"`
	ToTarget                  StreamWaitSummary        `json:"to_target"`
	UntrackedStreams          uint64                   `json:"untracked_streams_total"`
	FastPathUnobservedStreams uint64                   `json:"fast_path_unobserved_streams_total"`
}

const (
	streamPhaseInitial uint32 = iota
	streamPhaseRead
	streamPhaseAccounting
	streamPhaseWrite
	streamPhaseFinished
)

var streamPhaseNames = [...]string{"initial", "read", "accounting", "write", "finished"}

type streamDirection struct {
	created      time.Time
	owner        *evidenceStream
	direction    string
	phase        atomic.Uint32
	phaseAt      atomic.Int64
	readBytes    atomic.Uint64
	writtenBytes atomic.Uint64
	pending      atomic.Int64
	maxWriteNS   atomic.Int64
	shortWrites  atomic.Uint64
	finished     atomic.Bool
	errorClass   atomic.Pointer[string]
}

func (s *streamDirection) begin(phase uint32) time.Time {
	now := time.Now()
	s.phaseAt.Store(now.Sub(s.created).Nanoseconds())
	s.phase.Store(phase)
	return now
}

func (s *streamDirection) read(n int) {
	if n > 0 {
		s.readBytes.Add(uint64(n))
		s.pending.Store(int64(n))
	}
}

func (s *streamDirection) wrote(n, wanted int, started time.Time, err error) {
	if n > 0 {
		s.writtenBytes.Add(uint64(n))
	}
	s.pending.Store(int64(max(0, wanted-n)))
	if n < wanted {
		s.shortWrites.Add(1)
	}
	elapsed := time.Since(started).Nanoseconds()
	for old := s.maxWriteNS.Load(); elapsed > old && !s.maxWriteNS.CompareAndSwap(old, elapsed); old = s.maxWriteNS.Load() {
	}
	s.completeOperation("write", started, wanted, n, err)
}

func classifyStreamError(err error) string {
	class := "none"
	var ne net.Error
	var se *quic.StreamError
	var ae *quic.ApplicationError
	switch {
	case err == nil:
		class = "none"
	case errors.Is(err, io.EOF):
		class = "eof"
	case errors.Is(err, errDisconnect):
		class = "traffic_denied"
	case errors.Is(err, net.ErrClosed) || (errors.As(err, &se) && se.ErrorCode == 0) || (errors.As(err, &ae) && ae.ErrorCode == closeErrCodeOK):
		class = "closed"
	case errors.As(err, &ne) && ne.Timeout():
		class = "timeout"
	default:
		class = "other"
	}
	return class
}

func (s *streamDirection) completeOperation(phase string, started time.Time, requested, written int, err error) {
	elapsed := time.Since(started)
	if elapsed < time.Second || s.owner == nil {
		return
	}
	s.owner.slowObserved.Store(true)
	captured := s.created.Add(time.Since(s.created))
	e := s.owner.connection.snapshot("stream_operation_slow_complete", captured)
	e.Operation = &StreamOperationEvidence{StreamID: s.owner.id, Direction: s.direction, Phase: phase,
		StartedAt: s.created.Add(started.Sub(s.created)).UTC(), DurationUS: elapsed.Microseconds(), RequestedBytes: requested, WrittenBytes: written, ErrorClass: classifyStreamError(err)}
	s.owner.manager.enqueue(e)
}

func (s *streamDirection) finish(err error) {
	class := classifyStreamError(err)
	s.errorClass.Store(&class)
	s.pending.Store(0)
	s.begin(streamPhaseFinished)
	if !s.finished.Swap(true) && s.owner != nil {
		for i, name := range []string{"none", "eof", "closed", "traffic_denied", "timeout", "other"} {
			if class == name {
				s.owner.connection.terminalDirections[i].Add(1)
				break
			}
		}
		// A direction can return after first-direction close removed the slot. Do not
		// let routine-close suppression conceal a late error or slow completion.
		if s.owner.ended.Load() && s.owner.interesting(time.Now()) {
			now := time.Now()
			e := s.owner.manager.connectionSnapshot(s.owner.connection, "stream_terminal_update", now)
			snapshot := s.owner.snapshot(now)
			e.Stream = &snapshot
			s.owner.manager.enqueue(e)
		}
	}
}

func (s *streamDirection) snapshot(now time.Time) StreamDirectionEvidence {
	phase := s.phase.Load()
	e := StreamDirectionEvidence{Phase: streamPhaseNames[phase], PhaseAgeMS: evidenceAge(now, s.created, s.phaseAt.Load()),
		ReadBytes: s.readBytes.Load(), WrittenBytes: s.writtenBytes.Load(), PendingBytes: s.pending.Load(),
		MaxCompletedWriteUS: s.maxWriteNS.Load() / int64(time.Microsecond), ShortWrites: s.shortWrites.Load(), Finished: s.finished.Load()}
	if class := s.errorClass.Load(); class != nil {
		e.ErrorClass = *class
	}
	return e
}

type evidenceStream struct {
	manager      *quicEvidence
	connection   *evidenceState
	id           uint64
	created      time.Time
	toClient     streamDirection
	toTarget     streamDirection
	ended        atomic.Bool
	slowObserved atomic.Bool
}

func (s *evidenceStream) snapshot(now time.Time) StreamEvidence {
	e := StreamEvidence{ID: s.id, AgeMS: max(0, now.Sub(s.created).Milliseconds()), ToClient: s.toClient.snapshot(now), ToTarget: s.toTarget.snapshot(now)}
	e.BestEffort = !e.ToClient.Finished || !e.ToTarget.Finished
	return e
}

func (s *evidenceStream) slow(now time.Time) bool {
	for _, direction := range []*streamDirection{&s.toClient, &s.toTarget} {
		phase := direction.phase.Load()
		if (phase == streamPhaseWrite || phase == streamPhaseAccounting) && direction.pending.Load() > 0 && evidenceAge(now, s.created, direction.phaseAt.Load()) >= 1000 {
			return true
		}
	}
	return false
}

func (s *evidenceStream) interesting(now time.Time) bool {
	if s.slowObserved.Load() || s.slow(now) {
		return true
	}
	for _, d := range []*streamDirection{&s.toClient, &s.toTarget} {
		if d.shortWrites.Load() > 0 || d.maxWriteNS.Load() >= int64(time.Second) {
			return true
		}
		if c := d.errorClass.Load(); c != nil && *c != "none" && *c != "eof" && *c != "closed" {
			return true
		}
	}
	return false
}

func (m *quicEvidence) traceStream(ctx context.Context) *evidenceStream {
	if m == nil || !m.config.StreamEvidence {
		return nil
	}
	id, ok := ctx.Value(quic.ConnectionTracingKey).(quic.ConnectionTracingID)
	if !ok {
		return nil
	}
	m.mu.Lock()
	connection := m.states[id]
	if connection == nil || !connection.authenticated.Load() || connection.ended.Load() {
		m.mu.Unlock()
		return nil
	}
	if len(m.streams) >= m.config.MaxStreams || connection.activeStreams >= m.config.MaxStreamsPerConnection {
		connection.untrackedStreams.Add(1)
		m.mu.Unlock()
		return nil
	}
	m.nextStream++
	now := time.Now()
	stream := &evidenceStream{manager: m, connection: connection, id: m.nextStream, created: now}
	stream.toClient.created, stream.toTarget.created = now, now
	stream.toClient.owner, stream.toTarget.owner = stream, stream
	stream.toClient.direction, stream.toTarget.direction = "to_client", "to_target"
	stream.toClient.phaseAt.Store(-1)
	stream.toTarget.phaseAt.Store(-1)
	m.streams[stream.id] = stream
	connection.activeStreams++
	m.mu.Unlock()
	return stream
}

func (m *quicEvidence) markFastPath(ctx context.Context) {
	if m == nil || !m.config.StreamEvidence {
		return
	}
	id, ok := ctx.Value(quic.ConnectionTracingKey).(quic.ConnectionTracingID)
	if !ok {
		return
	}
	m.mu.Lock()
	if s := m.states[id]; s != nil && s.authenticated.Load() {
		s.fastPathStreams.Add(1)
	}
	m.mu.Unlock()
}

func (s *evidenceStream) close() {
	if s == nil {
		return
	}
	m := s.manager
	m.mu.Lock()
	if s.ended.Swap(true) {
		m.mu.Unlock()
		return
	}
	delete(m.streams, s.id)
	s.connection.activeStreams--
	m.mu.Unlock()
	now := time.Now()
	s.connection.completedStreams.Add(1)
	if !s.toClient.finished.Load() || !s.toTarget.finished.Load() {
		s.connection.incompleteCloses.Add(1)
	}
	if !s.interesting(now) {
		s.connection.suppressedCloses.Add(1)
		return
	}
	e := m.connectionSnapshot(s.connection, "stream_closed", now)
	stream := s.snapshot(now)
	e.Stream = &stream
	m.enqueue(e)
}

func (m *quicEvidence) closeConnectionStreams(connection *evidenceState) {
	if !m.config.StreamEvidence {
		return
	}
	m.mu.Lock()
	var streams []*evidenceStream
	for _, s := range m.streams {
		if s.connection == connection {
			streams = append(streams, s)
		}
	}
	m.mu.Unlock()
	for _, s := range streams {
		s.close()
	}
}

func addStreamWait(summary *StreamWaitSummary, e StreamDirectionEvidence) {
	switch e.Phase {
	case "write":
		summary.WritingStreams++
		summary.PendingBytes += e.PendingBytes
		summary.MaxWriteAgeMS = max(summary.MaxWriteAgeMS, e.PhaseAgeMS)
	case "accounting":
		summary.AccountingStreams++
		summary.PendingBytes += e.PendingBytes
		summary.MaxAccountingAgeMS = max(summary.MaxAccountingAgeMS, e.PhaseAgeMS)
	}
}

func (m *quicEvidence) connectionSnapshot(s *evidenceState, event string, now time.Time) QUICEvidence {
	e := s.snapshot(event, now)
	if !m.config.StreamEvidence {
		return e
	}
	app := &ApplicationEvidence{CompletedStreams: s.completedStreams.Load(), IncompleteCloses: s.incompleteCloses.Load(), SuppressedRoutineCloses: s.suppressedCloses.Load(), TerminalDirections: StreamTerminalDirections{None: s.terminalDirections[0].Load(), EOF: s.terminalDirections[1].Load(), Closed: s.terminalDirections[2].Load(), TrafficDenied: s.terminalDirections[3].Load(), Timeout: s.terminalDirections[4].Load(), Other: s.terminalDirections[5].Load()}, UntrackedStreams: s.untrackedStreams.Load(), FastPathUnobservedStreams: s.fastPathStreams.Load()}
	m.mu.Lock()
	for _, stream := range m.streams {
		if stream.connection != s {
			continue
		}
		app.ObservedActiveStreams++
		addStreamWait(&app.ToClient, stream.toClient.snapshot(now))
		addStreamWait(&app.ToTarget, stream.toTarget.snapshot(now))
	}
	m.mu.Unlock()
	e.Application = app
	return e
}
