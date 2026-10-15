//go:build linux

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/apernet/hysteria/core/v2/client"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
)

type h2TrafficTotals struct{ Tx, Rx uint64 }

func h2Traffic(t *testing.T, w *h1Worker) h2TrafficTotals {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, w.ready.Control+"/traffic", nil)
	req.Header.Set("Authorization", "synthetic-management-only")
	hc := &http.Client{Timeout: 2 * time.Second}
	resp, e := hc.Do(req)
	if e != nil {
		t.Fatal(e)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("traffic status=%d", resp.StatusCode)
	}
	var m map[string]h2TrafficTotals
	if e = json.NewDecoder(resp.Body).Decode(&m); e != nil {
		t.Fatal(e)
	}
	var total h2TrafficTotals
	for _, v := range m {
		total.Tx += v.Tx
		total.Rx += v.Rx
	}
	return total
}
func TestH2AccountingNetwork(t *testing.T) {
	if os.Getenv("PCS_H2_ACCOUNTING") != "1" {
		t.Skip("opt-in accounting acceptance")
	}
	for _, n := range []int{1, 4} {
		t.Run(fmt.Sprintf("connections_%d", n), func(t *testing.T) {
			ca := h1Certificate(t)
			w := h1StartWorker(t, ca, true, true, 30)
			defer w.close()
			up, down := h2NewLink(250000), h2NewLink(500000)
			defer up.close()
			defer down.close()
			target := h1UDPEcho(t)
			var cs []client.Client
			for i := 0; i < n; i++ {
				relay := h2NewRelay(t, w.ready.UDP, up, down)
				c, e := h1Client(relay.front.LocalAddr().String(), ca.roots, "pcs-h1.test", h1TestAuth, 30)
				if e != nil {
					t.Fatal(e)
				}
				cs = append(cs, c)
				defer c.Close()
			}
			before := h2Traffic(t, w)
			start := time.Now()
			var received uint64
			for _, c := range cs {
				u, e := c.UDP()
				if e != nil {
					t.Fatal(e)
				}
				for _, size := range []int{1000, 1350, 3000} {
					if e = h1UDPRoundTrip(u, target, bytes.Repeat([]byte{0x5a}, size)); e != nil {
						t.Fatal(e)
					}
					received += uint64(size)
				}
				u.Close()
			}
			time.Sleep(1100 * time.Millisecond)
			after := h2Traffic(t, w)
			ev := h2Totals(w.metrics(t).Evidence)
			tx, rx := after.Tx-before.Tx, after.Rx-before.Rx
			expectedRx := received
			if os.Getenv("PCS_H2_EXPECT_ACCOUNTING") == "duplicate_baseline" {
				expectedRx = uint64(n) * 9700
			}
			out := map[string]any{"variant": os.Getenv("PCS_H2_VARIANT"), "connections": n, "started_at": start.UTC(), "finished_at": time.Now().UTC(), "client_received_exact_bytes": received, "API_upload_delta": tx, "API_download_delta": rx, "API_download_expected": expectedRx, "evidence": ev, "up": up.snapshot(), "down": down.snapshot()}
			body, _ := json.Marshal(out)
			t.Logf("H2_ACCOUNTING_RESULT %s", body)
			if tx != received || rx != expectedRx {
				t.Fatalf("accounting mismatch: tx=%d rx=%d received=%d wantRx=%d", tx, rx, received, expectedRx)
			}
			if ev.UDPRead != received || ev.UDPQueued != received || ev.UDPFragments != uint64(n*2) || ev.UDPTooLarge != uint64(n*2) {
				t.Fatal("fragment evidence changed or incomplete")
			}
		})
	}
}
func TestH2AccountingFragmentDenial(t *testing.T) {
	if os.Getenv("PCS_H2_ACCOUNTING") != "1" {
		t.Skip("opt-in accounting acceptance")
	}
	ca := h1Certificate(t)
	w := h1StartWorker(t, ca, true, true, 30)
	defer w.close()
	up, down := h2NewLink(250000), h2NewLink(500000)
	defer up.close()
	defer down.close()
	r := h2NewRelay(t, w.ready.UDP, up, down)
	c, e := h1Client(r.front.LocalAddr().String(), ca.roots, "pcs-h1.test", h1TestAuth, 30)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	u, e := c.UDP()
	if e != nil {
		t.Fatal(e)
	}
	defer u.Close()
	target, e := net.ListenPacket("udp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	defer target.Close()
	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		b := make([]byte, 4096)
		n, addr, e := target.ReadFrom(b)
		if e != nil {
			return
		}
		close(entered)
		<-release
		_, _ = target.WriteTo(b[:n], addr)
	}()
	if e = u.Send(bytes.Repeat([]byte{0x5a}, 1350), target.LocalAddr().String()); e != nil {
		t.Fatal(e)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("gated UDP target not reached")
	}
	before := h2Traffic(t, w)
	req, _ := http.NewRequest(http.MethodPost, w.ready.Control+"/kick", bytes.NewBufferString(`["synthetic-session-identity"]`))
	req.Header.Set("Authorization", "synthetic-management-only")
	hc := &http.Client{Timeout: 2 * time.Second}
	resp, e := hc.Do(req)
	if e != nil {
		close(release)
		t.Fatal(e)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		close(release)
		t.Fatalf("kick status=%d", resp.StatusCode)
	}
	close(release)
	<-done
	receive := make(chan error, 1)
	go func() { _, _, err := u.Receive(); receive <- err }()
	select {
	case err := <-receive:
		if err == nil {
			t.Fatal("denied fragmented payload reached client")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("denied connection did not close")
	}
	time.Sleep(1100 * time.Millisecond)
	after := h2Traffic(t, w)
	ev := h2Totals(w.metrics(t).Evidence)
	out := map[string]any{"variant": os.Getenv("PCS_H2_VARIANT"), "client_rejected_payload": true, "API_download_delta": after.Rx - before.Rx, "evidence": ev}
	body, _ := json.Marshal(out)
	t.Logf("H2_ACCOUNTING_DENIAL_RESULT %s", body)
	if after.Rx != before.Rx || ev.UDPRead != 1350 || ev.UDPQueued != 0 || ev.UDPTooLarge != 0 || ev.UDPFragments != 0 {
		t.Fatal("denied original reached fragmentation/accounting/send path")
	}
}
