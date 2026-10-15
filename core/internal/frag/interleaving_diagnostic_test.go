package frag

import (
	"bytes"
	"os"
	"testing"

	"github.com/apernet/hysteria/core/v2/internal/protocol"
)

// The diagnostic has no network, timing or concurrency dependency. It records
// the current single-packet defrag behavior without making data loss a desired
// contract. The environment gate is for a future bounded reorder-tolerant fix.
func TestDefragInterleavingDiagnostic(t *testing.T) {
	for _, tc := range []struct {
		name       string
		order      []int
		complete   int
		diagnostic bool
	}{
		{"in_order", []int{0, 1, 2, 3}, 2, false},
		{"within_packet_reorder", []int{1, 0}, 1, false},
		{"duplicate_fragment", []int{0, 0, 1}, 1, false},
		{"interleaved_complete_packets", []int{0, 2, 1, 3}, 2, true},
		{"missing_fragment_then_complete_packet", []int{0, 2, 3}, 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payloads := map[uint16][]byte{1: []byte("AAAAaaaa"), 2: []byte("BBBBbbbb")}
			var fragments []protocol.UDPMessage
			for _, id := range []uint16{1, 2} {
				message := &protocol.UDPMessage{SessionID: 7, PacketID: id, FragCount: 1, Addr: "fixture.invalid:443", Data: payloads[id]}
				fragments = append(fragments, FragUDPMessage(message, message.HeaderSize()+4)...)
			}
			var d Defragger
			seen := make(map[uint16]bool)
			for _, index := range tc.order {
				if message := d.Feed(&fragments[index]); message != nil {
					if !bytes.Equal(message.Data, payloads[message.PacketID]) || message.SessionID != 7 {
						t.Fatal("wrong payload or cross-session result")
					}
					if seen[message.PacketID] {
						t.Fatal("duplicate packet delivery")
					}
					seen[message.PacketID] = true
				}
			}
			if tc.diagnostic {
				t.Logf("all four fragments delivered as A0,B0,A1,B1; complete_packets=%d expected_for_reorder_isolation=%d", len(seen), tc.complete)
				if os.Getenv("PCS_REQUIRE_INTERLEAVED_DEFRAG") != "1" {
					return
				}
			}
			if len(seen) != tc.complete {
				t.Fatalf("complete packets=%d, want %d", len(seen), tc.complete)
			}
		})
	}
}
