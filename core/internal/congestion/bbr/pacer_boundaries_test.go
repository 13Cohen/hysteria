package bbr

import (
	"github.com/apernet/quic-go/congestion"
	"github.com/apernet/quic-go/logging"
	"testing"
	"time"
)

func TestCalculatedPacerBoundaryStates(t *testing.T) {
	now := time.Unix(1700000000, 0)
	for _, tc := range []struct {
		name     string
		rtt      time.Duration
		mode     bbrMode
		recovery bbrRecoveryState
		estimate Bandwidth
		gain     float64
	}{
		{"startup_no_estimate_no_rtt", 0, bbrModeStartup, bbrRecoveryStateNotInRecovery, 0, defaultHighGain},
		{"startup_no_estimate_160ms", 160 * time.Millisecond, bbrModeStartup, bbrRecoveryStateNotInRecovery, 0, defaultHighGain},
		{"probe_rtt", 160 * time.Millisecond, bbrModeProbeRtt, bbrRecoveryStateNotInRecovery, 800000, 1},
		{"recovery_conservation", 160 * time.Millisecond, bbrModeProbeBw, bbrRecoveryStateConservation, 800000, 0.75},
		{"recovery_growth", 160 * time.Millisecond, bbrModeProbeBw, bbrRecoveryStateGrowth, 800000, 1.25},
		{"low_estimate_floor", 160 * time.Millisecond, bbrModeProbeBw, bbrRecoveryStateNotInRecovery, 8000, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := NewBbrSender(diagnosticClock{now}, congestion.InitialPacketSize)
			rtt := &logging.RTTStats{}
			if tc.rtt > 0 {
				rtt.UpdateRTT(tc.rtt, 0)
			}
			b.SetRTTStatsProvider(rtt)
			b.mode = tc.mode
			b.recoveryState = tc.recovery
			b.recoveryWindow = b.minCongestionWindow
			b.pacingGain = tc.gain
			if tc.estimate > 0 {
				b.maxBandwidth.Update(tc.estimate, 1)
				b.isAtFullBandwidth = true
				b.calculatePacingRate(0)
			}
			rate := b.bandwidthForPacer()
			want := max(congestion.ByteCount(b.PacingRate()/BytesPerSecond), congestion.ByteCount(minBps))
			if rate != want || rate <= 0 {
				t.Fatalf("invalid calculated pacing: got %d want %d", rate, want)
			}
			if (tc.mode == bbrModeProbeRtt || tc.recovery != bbrRecoveryStateNotInRecovery) && b.GetCongestionWindow() != b.minCongestionWindow {
				t.Fatal("minimum congestion window boundary changed")
			}
			for i := 0; i < 10; i++ {
				b.OnPacketSent(now, congestion.ByteCount((i+1)*1280), congestion.PacketNumber(i), 1280, true)
			}
			wait := b.TimeUntilSend(12800).Sub(now)
			if wait <= 0 || wait > time.Second || !b.HasPacingBudget(now.Add(wait)) {
				t.Fatalf("invalid pacing progress at boundary: %s", wait)
			}
			t.Logf("state=%s actual_pacer_Bps=%d effective_cwnd=%d wait_ns=%d", tc.name, rate, b.GetCongestionWindow(), wait.Nanoseconds())
		})
	}
}
