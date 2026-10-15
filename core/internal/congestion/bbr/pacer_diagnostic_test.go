package bbr

import (
	"os"
	"testing"
	"time"

	"github.com/apernet/quic-go/congestion"
)

// This diagnostic exercises the production pacer with fake time, not a model
// of its token bucket. It deliberately does not change any congestion behavior.
type diagnosticClock struct{ now time.Time }

func (c diagnosticClock) Now() time.Time { return c.now }

func TestPacerGainDiagnostic(t *testing.T) {
	const estimate = Bandwidth(800000) // 100,000 bytes/s, above the floor at gain 1.
	for _, tc := range []struct {
		name string
		mode bbrMode
		gain float64
	}{
		{"probe_bw_high", bbrModeProbeBw, 1.25},
		{"probe_bw_low", bbrModeProbeBw, 0.75},
		{"probe_bw_cruise", bbrModeProbeBw, 1.0},
		{"drain", bbrModeDrain, 1.0 / defaultHighGain},
	} {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Unix(1700000000, 0)
			b := NewBbrSender(diagnosticClock{now}, congestion.InitialPacketSize)
			b.mode = tc.mode
			b.maxBandwidth.Update(estimate, 1)
			b.isAtFullBandwidth = true
			b.pacingGain = tc.gain
			b.congestionWindowGain = 2
			b.calculatePacingRate(0)
			calculatedBytesPerSecond := congestion.ByteCount(b.PacingRate() / BytesPerSecond)
			actualBytesPerSecond := b.bandwidthForPacer()
			if b.PacingRate() != Bandwidth(tc.gain*float64(estimate)) {
				t.Fatal("fixture did not update the calculated rate")
			}

			// Exhaust the real pacer's initial ten-packet allowance. No sleeping,
			// goroutines, sockets, RTT provider, or congestion events are required.
			for pn := 0; pn < 10; pn++ {
				b.OnPacketSent(now, congestion.ByteCount((pn+1)*1280), congestion.PacketNumber(pn), 1280, true)
			}
			if b.HasPacingBudget(now) {
				t.Fatal("fixture failed to exhaust the initial burst allowance")
			}
			wait := b.TimeUntilSend(12800).Sub(now)
			expectedWait := time.Duration((int64(1280)*int64(time.Second) + int64(actualBytesPerSecond) - 1) / int64(actualBytesPerSecond))
			expectedWait = max(expectedWait, congestion.MinPacingDelay)
			if wait != expectedWait || b.HasPacingBudget(now.Add(wait-time.Nanosecond)) || !b.HasPacingBudget(now.Add(wait)) {
				t.Fatalf("pacer execution differs from its callback: actual wait=%s callback wait=%s", wait, expectedWait)
			}
			t.Logf("mode=%s pacing_gain=%.9f cwnd_gain=2 calculated_Bps=%d actual_pacer_Bps=%d actual_next_packet_wait_ns=%d", tc.name, tc.gain, calculatedBytesPerSecond, actualBytesPerSecond, wait.Nanoseconds())

			// Opt-in future acceptance gate: the current upstream implementation
			// uses the calculated pacing rate. Baseline v2.6.4 fails this gate.
			if os.Getenv("PCS_REQUIRE_CALCULATED_PACING") == "1" {
				want := max(calculatedBytesPerSecond, congestion.ByteCount(minBps))
				if actualBytesPerSecond != want {
					t.Fatalf("calculated pacing gate: actual=%d want=%d bytes/s", actualBytesPerSecond, want)
				}
			}
		})
	}
}

func TestPacerFloorUnitsDiagnostic(t *testing.T) {
	now := time.Unix(1700000000, 0)
	b := NewBbrSender(diagnosticClock{now}, congestion.InitialPacketSize)
	b.maxBandwidth.Update(Bandwidth(8000), 1)
	b.congestionWindowGain = 2
	b.pacingGain = 1
	b.isAtFullBandwidth = true
	b.calculatePacingRate(0)
	actual := b.bandwidthForPacer()
	if actual != 65536 {
		t.Fatalf("baseline floor changed: actual=%d bytes/s", actual)
	}
	t.Logf("actual_floor_Bps=%d actual_floor_bps=%d (pacer consumes bytes/s)", actual, actual*8)
}
