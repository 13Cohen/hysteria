package server

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apernet/quic-go"
	"github.com/apernet/quic-go/logging"
)

type streamTestWriter func([]byte) (int, error)

func (f streamTestWriter) Write(p []byte) (int, error) { return f(p) }

type streamTestReader func([]byte) (int, error)

func (f streamTestReader) Read(p []byte) (int, error) { return f(p) }

func newTestStream(t *testing.T) (*quicEvidence, *evidenceStream) {
	t.Helper()
	m, err := newQUICEvidence(QUICEvidenceConfig{Enabled: true, StreamEvidence: true}, evidenceLoggerFunc(func(QUICEvidence) {}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := evidenceContext(1)
	m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
	m.authenticate(ctx, 1)
	<-m.events
	return m, m.traceStream(ctx)
}

func TestStreamEvidenceShortWritesPreserveSemantics(t *testing.T) {
	for _, writeErr := range []error{nil, io.ErrClosedPipe} {
		for _, observed := range []bool{false, true} {
			_, stream := newTestStream(t)
			var direction *streamDirection
			if observed {
				direction = &stream.toClient
			}
			calls := 0
			writer := streamTestWriter(func(p []byte) (int, error) { calls++; return 2, writeErr })
			err := copyBufferLogObserved(writer, bytes.NewBufferString("abcdef"), func(uint64) bool { return true }, direction)
			if !errors.Is(err, writeErr) || calls != 1 {
				t.Fatalf("short-write behavior changed: err=%v calls=%d", err, calls)
			}
			if observed {
				e := direction.snapshot(time.Now())
				if e.ReadBytes != 6 || e.WrittenBytes != 2 || e.ShortWrites != 1 || !e.Finished {
					t.Fatalf("bad actual-write evidence: %+v", e)
				}
			}
		}
	}
}

func TestStreamEvidenceAccountingAndWriteBlockAreSeparate(t *testing.T) {
	m, stream := newTestStream(t)
	accountingEntered, releaseAccounting := make(chan struct{}), make(chan struct{})
	writeEntered, releaseWrite := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- copyBufferLogObserved(streamTestWriter(func(p []byte) (int, error) { close(writeEntered); <-releaseWrite; return len(p), nil }), bytes.NewBufferString("pending"), func(uint64) bool { close(accountingEntered); <-releaseAccounting; return true }, &stream.toClient)
	}()
	<-accountingEntered
	e := m.connectionSnapshot(stream.connection, "sample", time.Now())
	if e.Application.ToClient.AccountingStreams != 1 || e.Application.ToClient.WritingStreams != 0 || e.Application.ToClient.PendingBytes != 7 {
		t.Fatalf("accounting was confused with write: %+v", e.Application)
	}
	close(releaseAccounting)
	<-writeEntered
	e = m.connectionSnapshot(stream.connection, "sample", time.Now())
	if e.Application.ToClient.WritingStreams != 1 || e.Application.ToClient.AccountingStreams != 0 || e.Application.ToClient.PendingBytes != 7 {
		t.Fatalf("missing pending write: %+v", e.Application)
	}
	close(releaseWrite)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	final := stream.snapshot(time.Now())
	if final.ToClient.ReadBytes != 7 || final.ToClient.WrittenBytes != 7 || !final.ToClient.Finished || !final.BestEffort {
		t.Fatalf("bad one-direction finish: %+v", final)
	}
	stream.toTarget.finish(nil)
	stream.slowObserved.Store(true)
	stream.close()
	closed := <-m.events
	if closed.Event != "stream_closed" || closed.Stream.BestEffort || closed.Stream.ID != stream.id {
		t.Fatalf("bad close evidence: %+v", closed)
	}
}

func TestStreamEvidenceReadWaitIsNotWriteBlock(t *testing.T) {
	_, stream := newTestStream(t)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- copyBufferLogObserved(io.Discard, streamTestReader(func([]byte) (int, error) { close(entered); <-release; return 0, io.EOF }), func(uint64) bool { return true }, &stream.toTarget)
	}()
	<-entered
	e := stream.snapshot(time.Now())
	if e.ToTarget.Phase != "read" || e.ToTarget.PendingBytes != 0 || stream.slow(time.Now().Add(2*time.Second)) {
		t.Fatalf("read wait mislabeled as blocked write: %+v", e)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestStreamEvidenceAccountingRejectDoesNotWrite(t *testing.T) {
	_, stream := newTestStream(t)
	writes := 0
	err := copyBufferLogObserved(streamTestWriter(func(p []byte) (int, error) { writes++; return len(p), nil }), bytes.NewBufferString("denied"), func(uint64) bool { return false }, &stream.toTarget)
	e := stream.toTarget.snapshot(time.Now())
	if !errors.Is(err, errDisconnect) || writes != 0 || e.ReadBytes != 6 || e.WrittenBytes != 0 || e.ErrorClass != "traffic_denied" {
		t.Fatalf("traffic rejection changed: %v %+v", err, e)
	}
}

func TestStreamEvidenceAuthenticationCapsAndFastCoverage(t *testing.T) {
	m, err := newQUICEvidence(QUICEvidenceConfig{Enabled: true, StreamEvidence: true, MaxStreams: 2, MaxStreamsPerConnection: 1}, evidenceLoggerFunc(func(QUICEvidence) {}))
	if err != nil {
		t.Fatal(err)
	}
	ctx := evidenceContext(1)
	tracer := m.tracer(ctx, logging.PerspectiveServer, quic.ConnectionID{})
	if m.traceStream(ctx) != nil {
		t.Fatal("unauthenticated stream traced")
	}
	m.authenticate(ctx, 1)
	<-m.events
	first := m.traceStream(ctx)
	if first == nil || m.traceStream(ctx) != nil {
		t.Fatal("per-session cap failed")
	}
	ctx2 := evidenceContext(2)
	m.tracer(ctx2, logging.PerspectiveServer, quic.ConnectionID{})
	m.authenticate(ctx2, 2)
	<-m.events
	second := m.traceStream(ctx2)
	if second == nil {
		t.Fatal("other session was starved before global cap")
	}
	m.markFastPath(ctx)
	e := m.connectionSnapshot(first.connection, "sample", time.Now())
	if e.Application.ObservedActiveStreams != 1 || e.Application.UntrackedStreams != 1 || e.Application.FastPathUnobservedStreams != 1 {
		t.Fatalf("bad coverage: %+v", e.Application)
	}
	tracer.Close()
	if len(m.streams) != 1 || !first.ended.Load() {
		t.Fatal("connection close retained stream slots")
	}
	second.close()
	if len(m.streams) != 0 {
		t.Fatal("stream close retained slot")
	}
}

func TestStreamEvidenceConcurrentCloseIsBestEffortAndBounded(t *testing.T) {
	m, stream := newTestStream(t)
	stream.slowObserved.Store(true)
	var wg sync.WaitGroup
	for _, direction := range []*streamDirection{&stream.toClient, &stream.toTarget} {
		wg.Add(1)
		go func(direction *streamDirection) {
			defer wg.Done()
			for n := 0; n < 1000; n++ {
				direction.begin(streamPhaseRead)
				direction.read(32)
				direction.begin(streamPhaseAccounting)
				direction.begin(streamPhaseWrite)
				direction.wrote(32, 32, time.Now(), nil)
			}
			direction.finish(nil)
		}(direction)
	}
	for n := 0; n < 100; n++ {
		_ = m.connectionSnapshot(stream.connection, "sample", time.Now())
		_ = stream.snapshot(time.Now())
	}
	stream.close()
	stream.close()
	wg.Wait()
	if len(m.streams) != 0 {
		t.Fatal("leaked slot")
	}
	closes := 0
	for len(m.events) > 0 {
		e := <-m.events
		if e.Event == "stream_closed" {
			closes++
		}
		if e.Stream == nil || e.Stream.BestEffort == (e.Stream.ToClient.Finished && e.Stream.ToTarget.Finished) {
			t.Fatal("finish-state bound misleading")
		}
	}
	if closes != 1 || stream.connection.completedStreams.Load() != 1 {
		t.Fatal("duplicate close")
	}

}

func TestStreamEvidenceConfigBounds(t *testing.T) {
	for _, cfg := range []QUICEvidenceConfig{{StreamEvidence: true}, {Enabled: true, StreamEvidence: true, MaxStreams: 1025}, {Enabled: true, StreamEvidence: true, MaxStreamsPerConnection: 129}} {
		if err := cfg.validate(evidenceLoggerFunc(func(QUICEvidence) {})); err == nil {
			t.Fatalf("accepted invalid stream config: %+v", cfg)
		}
	}
}

func TestStreamEvidenceCompletedSlowOperationBetweenSamples(t *testing.T) {
	for _, phase := range []string{"accounting", "write"} {
		t.Run(phase, func(t *testing.T) {
			m, stream := newTestStream(t) // No periodic worker runs: the completion event must stand alone.
			started := time.Now()
			writer := streamTestWriter(func(p []byte) (int, error) {
				if phase == "write" {
					time.Sleep(1200 * time.Millisecond)
				}
				return len(p), nil
			})
			err := copyBufferLogObserved(writer, bytes.NewBufferString("slow"), func(uint64) bool {
				if phase == "accounting" {
					time.Sleep(1200 * time.Millisecond)
				}
				return true
			}, &stream.toClient)
			if err != nil {
				t.Fatal(err)
			}
			if len(m.events) != 1 {
				t.Fatalf("completed slow operation lost or duplicated: %d", len(m.events))
			}
			e := <-m.events
			if e.Event != "stream_operation_slow_complete" || e.Operation == nil {
				t.Fatalf("missing completion event: %+v", e)
			}
			op := e.Operation
			expectedWritten := 4
			if phase == "accounting" {
				expectedWritten = 0
			}
			if op.Phase != phase || op.Direction != "to_client" || op.StreamID != stream.id || op.DurationUS < 1200000 || op.RequestedBytes != 4 || op.WrittenBytes != expectedWritten || op.ErrorClass != "none" {
				t.Fatalf("incorrect completed operation: %+v", op)
			}
			if op.StartedAt.Before(started.Add(-time.Millisecond)) || e.Time.Before(op.StartedAt.Add(time.Duration(op.DurationUS)*time.Microsecond)) {
				t.Fatalf("completion timing inconsistent: %+v", op)
			}
		})
	}
}

func TestStreamEvidenceRoutineCloseAndLateError(t *testing.T) {
	for _, lateErr := range []error{nil, io.EOF, &quic.StreamError{ErrorCode: 0}, &quic.ApplicationError{ErrorCode: closeErrCodeOK}, io.ErrUnexpectedEOF} {
		m, s := newTestStream(t)
		s.toClient.finish(nil)
		s.close()
		if len(m.events) != 0 || s.connection.completedStreams.Load() != 1 || s.connection.incompleteCloses.Load() != 1 || s.connection.suppressedCloses.Load() != 1 {
			t.Fatal("routine incomplete close not summarized")
		}
		s.toTarget.finish(lateErr)
		app := m.connectionSnapshot(s.connection, "sample", time.Now()).Application
		total := app.TerminalDirections.None + app.TerminalDirections.EOF + app.TerminalDirections.Closed + app.TerminalDirections.Other
		if total != 2 {
			t.Fatalf("direction completion denominator wrong %+v", app)
		}
		if lateErr == io.ErrUnexpectedEOF {
			if len(m.events) != 1 {
				t.Fatal("late abnormal terminal lost")
			}
			e := <-m.events
			if e.Event != "stream_terminal_update" || e.Stream.ID != s.id || e.Stream.BestEffort || e.Stream.ToTarget.ErrorClass != "other" {
				t.Fatal("late terminal inaccurate")
			}
		} else if len(m.events) != 0 {
			t.Fatal("normal EOF/zero-code close emitted full log")
		}
	}
}
