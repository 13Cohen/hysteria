package server

import (
	"context"
	"github.com/apernet/quic-go"
	"sync/atomic"
	"time"
)

// All counters belong to one authenticated QUIC connection. There is no extra
// per-UDP-session registry. Successful SendDatagram means queued, not delivered.
type UDPEvidence struct {
	LastTooLargeMaxDatagramPayloadSize int64             `json:"last_too_large_max_datagram_payload_size"`
	MinTooLargeMaxDatagramPayloadSize  int64             `json:"min_too_large_max_datagram_payload_size"`
	MaxTargetReadDatagramSize          int64             `json:"max_target_read_datagram_size"`
	WaitsBestEffort                    bool              `json:"waits_best_effort"`
	ReceiveQueueDropsObservable        bool              `json:"receive_queue_drops_observable"`
	ReceivedDatagrams                  uint64            `json:"received_datagrams"`
	ReceivedPayloadBytes               uint64            `json:"received_payload_bytes"`
	ParseErrors                        uint64            `json:"parse_errors"`
	TargetReadDatagrams                uint64            `json:"target_read_datagrams"`
	TargetReadBytes                    uint64            `json:"target_read_bytes"`
	TargetReadErrors                   uint64            `json:"target_read_errors"`
	TargetWriteCalls                   uint64            `json:"target_write_calls"`
	TargetWrittenBytes                 uint64            `json:"target_written_bytes"`
	TargetWriteErrors                  uint64            `json:"target_write_errors"`
	DefragPending                      uint64            `json:"defrag_pending"`
	SendAttempts                       uint64            `json:"send_attempts"`
	EnqueuedDatagrams                  uint64            `json:"enqueued_datagrams"`
	EnqueuedPayloadBytes               uint64            `json:"enqueued_payload_bytes"`
	SendTooLarge                       uint64            `json:"send_too_large"`
	SendErrors                         uint64            `json:"send_errors"`
	SerializeDrops                     uint64            `json:"serialize_drops"`
	FragmentedMessages                 uint64            `json:"fragmented_messages"`
	FragmentsAttempted                 uint64            `json:"fragments_attempted"`
	UntrackedOperations                uint64            `json:"untracked_operations_total"`
	Waits                              []UDPWaitEvidence `json:"waits"`
}
type UDPWaitEvidence struct {
	Direction    string `json:"direction"`
	Phase        string `json:"phase"`
	AgeMS        int64  `json:"age_ms"`
	PendingBytes int64  `json:"pending_bytes"`
}
type UDPOperationEvidence struct {
	Direction      string    `json:"direction"`
	Phase          string    `json:"phase"`
	StartedAt      time.Time `json:"started_at"`
	DurationUS     int64     `json:"duration_us"`
	RequestedBytes int64     `json:"requested_bytes"`
	WrittenBytes   int       `json:"written_bytes"`
	ErrorClass     string    `json:"error_class"`
}

const (
	udpPhaseReceiveAccounting uint32 = iota + 1
	udpPhaseSendAccounting
	udpPhaseSendDatagram
	udpPhaseFeed
	udpPhaseInit
	udpPhaseTargetWrite
)

var udpPhaseNames = [...]string{"", "receive_accounting", "send_accounting", "send_datagram", "feed", "init", "target_write"}

type udpOperation struct {
	busy      atomic.Bool
	phase     atomic.Uint32
	started   atomic.Int64
	requested atomic.Int64
	owner     *udpEvidence // fixed before the operation is first published
}
type udpEvidence struct {
	lastTooLargeMaxPayload, minTooLargeMaxPayload, maxTargetReadSize                                atomic.Int64
	manager                                                                                         *quicEvidence
	connection                                                                                      *evidenceState
	receivedDatagrams, receivedPayloadBytes, parseErrors                                            atomic.Uint64
	targetReadDatagrams, targetReadBytes, targetReadErrors                                          atomic.Uint64
	targetWriteCalls, targetWrittenBytes, targetWriteErrors, defragPending                          atomic.Uint64
	sendAttempts, enqueuedDatagrams, enqueuedPayloadBytes, sendTooLarge, sendErrors, serializeDrops atomic.Uint64
	fragmentedMessages, fragmentsAttempted, untrackedOperations                                     atomic.Uint64
	operations                                                                                      [8]udpOperation
}

func (m *quicEvidence) traceUDP(ctx context.Context) *udpEvidence {
	if m == nil || !m.config.UDPEvidence {
		return nil
	}
	id, ok := ctx.Value(quic.ConnectionTracingKey).(quic.ConnectionTracingID)
	if !ok {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.states[id]
	if s == nil || !s.authenticated.Load() || s.ended.Load() {
		return nil
	}
	return s.udp
}
func udpDirection(phase uint32) string {
	if phase == udpPhaseSendAccounting || phase == udpPhaseSendDatagram {
		return "to_client"
	}
	return "to_target"
}
func (u *udpEvidence) begin(phase uint32, n int) *udpOperation {
	if u == nil {
		return nil
	}
	for i := range u.operations {
		op := &u.operations[i]
		if op.busy.CompareAndSwap(false, true) {
			op.requested.Store(int64(n))
			op.started.Store(time.Since(u.connection.created).Nanoseconds())
			op.phase.Store(phase)
			return op
		}
	}
	u.untrackedOperations.Add(1)
	return nil
}
func (op *udpOperation) complete(n int, err error) {
	if op == nil {
		return
	}
	u := op.owner
	phase := op.phase.Load()
	started := op.started.Load()
	elapsed := time.Since(u.connection.created) - time.Duration(started)
	if elapsed >= time.Second {
		e := u.connection.snapshot("udp_operation_slow_complete", u.connection.created.Add(time.Since(u.connection.created)))
		e.UDPOperation = &UDPOperationEvidence{Direction: udpDirection(phase), Phase: udpPhaseNames[phase], StartedAt: u.connection.created.Add(time.Duration(started)).UTC(), DurationUS: elapsed.Microseconds(), RequestedBytes: op.requested.Load(), WrittenBytes: n, ErrorClass: classifyStreamError(err)}
		u.manager.enqueue(e)
	}
	op.phase.Store(0)
	op.busy.Store(false)
}
func (u *udpEvidence) snapshot(now time.Time) *UDPEvidence {
	e := &UDPEvidence{LastTooLargeMaxDatagramPayloadSize: u.lastTooLargeMaxPayload.Load(), MinTooLargeMaxDatagramPayloadSize: u.minTooLargeMaxPayload.Load(), MaxTargetReadDatagramSize: u.maxTargetReadSize.Load(), WaitsBestEffort: true, ReceivedDatagrams: u.receivedDatagrams.Load(), ReceivedPayloadBytes: u.receivedPayloadBytes.Load(), ParseErrors: u.parseErrors.Load(), TargetReadDatagrams: u.targetReadDatagrams.Load(), TargetReadBytes: u.targetReadBytes.Load(), TargetReadErrors: u.targetReadErrors.Load(), TargetWriteCalls: u.targetWriteCalls.Load(), TargetWrittenBytes: u.targetWrittenBytes.Load(), TargetWriteErrors: u.targetWriteErrors.Load(), DefragPending: u.defragPending.Load(), SendAttempts: u.sendAttempts.Load(), EnqueuedDatagrams: u.enqueuedDatagrams.Load(), EnqueuedPayloadBytes: u.enqueuedPayloadBytes.Load(), SendTooLarge: u.sendTooLarge.Load(), SendErrors: u.sendErrors.Load(), SerializeDrops: u.serializeDrops.Load(), FragmentedMessages: u.fragmentedMessages.Load(), FragmentsAttempted: u.fragmentsAttempted.Load(), UntrackedOperations: u.untrackedOperations.Load(), Waits: []UDPWaitEvidence{}}
	for i := range u.operations {
		op := &u.operations[i]
		phase := op.phase.Load()
		if phase != 0 {
			e.Waits = append(e.Waits, UDPWaitEvidence{Direction: udpDirection(phase), Phase: udpPhaseNames[phase], AgeMS: evidenceAge(now, u.connection.created, op.started.Load()), PendingBytes: op.requested.Load()})
		}
	}
	return e
}

// Optional internal interface leaves existing udpIO implementations unchanged.
type udpEvidenceProvider interface{ udpEvidenceState() *udpEvidence }

func evidenceForUDP(io udpIO) *udpEvidence {
	if p, ok := io.(udpEvidenceProvider); ok {
		return p.udpEvidenceState()
	}
	return nil
}
func (io *udpIOImpl) udpEvidenceState() *udpEvidence { return io.Evidence }

func (u *udpEvidence) tooLarge(maxPayload int64) {
	u.sendTooLarge.Add(1)
	u.lastTooLargeMaxPayload.Store(maxPayload)
	for old := u.minTooLargeMaxPayload.Load(); (old < 0 || maxPayload < old) && !u.minTooLargeMaxPayload.CompareAndSwap(old, maxPayload); old = u.minTooLargeMaxPayload.Load() {
	}
}
