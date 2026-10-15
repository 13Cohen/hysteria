//go:build linux

package cmd

import (
	"encoding/json"
	"os"
	"strconv"

	"github.com/apernet/hysteria/core/v2/client"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// This separately planned diagnostic changes only bulk size versus H3.
func TestH4WarmTwoMiB(t *testing.T) {
	if os.Getenv("PCS_H4_WARM") != "1" {
		t.Skip("opt-in warm four-connection2MiB diagnostic")
	}
	connections, e := strconv.Atoi(os.Getenv("PCS_H4_CONNECTIONS"))
	if e != nil || connections != 4 {
		t.Fatal("connections must be4")
	}
	ca := h1Certificate(t)
	w := h1StartWorker(t, ca, true, true, 60)
	defer w.close()
	up, down := h2NewLink(250000), h2NewLink(500000)
	defer up.close()
	defer down.close()
	target := h1Target(t, nil, nil)
	cs := make([]client.Client, connections)
	for i := range cs {
		relay := h2NewRelay(t, w.ready.UDP, up, down)
		c, err := h1Client(relay.front.LocalAddr().String(), ca.roots, "pcs-h1.test", h1TestAuth, 60)
		if err != nil {
			t.Fatal(err)
		}
		cs[i] = c
		defer c.Close()
		if _, err = h1Download(c, target, uint64(256<<10)/uint64(connections)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(1100 * time.Millisecond)
	// Resume every connection concurrently before CPU measurement. Warmup totals
	// stay fixed across connection counts, as does the measured payload below.
	var resume sync.WaitGroup
	var resumeFailures atomic.Uint64
	for _, c := range cs {
		resume.Add(1)
		go func(c client.Client) {
			defer resume.Done()
			if _, err := h1Download(c, target, uint64(64<<10)/uint64(connections)); err != nil {
				resumeFailures.Add(1)
			}
		}(c)
	}
	resume.Wait()
	if resumeFailures.Load() != 0 {
		t.Fatal("resume failed")
	}
	before := w.metrics(t)
	cgBefore := h1CgroupSnapshot()
	cpuBefore := h1HarnessCPU()
	u0, d0 := up.snapshot(), down.snapshot()
	begin := time.Now()
	var wg sync.WaitGroup
	var received atomic.Uint64
	var failed atomic.Uint64
	var latencies []float64
	var latencyMu sync.Mutex
	for _, c := range cs {
		wg.Add(2)
		go func(c client.Client) {
			defer wg.Done()
			n := uint64(2<<20) / uint64(connections)
			if _, e := h1Download(c, target, n); e != nil {
				failed.Add(1)
			} else {
				received.Add(n)
			}
		}(c)
		go func(c client.Client) {
			defer wg.Done()
			for i := 0; i < 16/connections; i++ {
				d, e := h1Download(c, target, 8192)
				if e != nil {
					failed.Add(1)
					return
				}
				received.Add(8192)
				latencyMu.Lock()
				latencies = append(latencies, float64(d)/float64(time.Millisecond))
				latencyMu.Unlock()
			}
		}(c)
	}
	wg.Wait()
	completed := time.Now()
	completionMetrics := w.metrics(t)
	time.Sleep(1100 * time.Millisecond)
	after := w.metrics(t)
	end := time.Now()
	harnessCPU := h1HarnessCPU() - cpuBefore
	cgAfter := h1CgroupSnapshot()
	delta := func(x h1Metrics) map[string]any {
		return map[string]any{"sample_start": before.CPUSampledAt.UTC(), "sample_end": x.CPUSampledAt.UTC(), "CPU_sample_wall_seconds": x.CPUSampledAt.Sub(before.CPUSampledAt).Seconds(), "CPU_seconds": x.CPUSeconds - before.CPUSeconds, "user_CPU_seconds": x.UserCPUSeconds - before.UserCPUSeconds, "system_CPU_seconds": x.SystemCPUSeconds - before.SystemCPUSeconds, "voluntary_context_switches": x.VoluntarySwitches - before.VoluntarySwitches, "involuntary_context_switches": x.InvoluntarySwitches - before.InvoluntarySwitches, "log_bytes": x.LogBytes - before.LogBytes, "alloc_bytes": x.TotalAlloc - before.TotalAlloc}
	}
	var p50, p95 any
	if len(latencies) > 0 {
		p50 = h1Percentile(latencies, .5)
		p95 = h1Percentile(latencies, .95)
	}
	// Preserve existing1Hz H1 gauges without enabling BBR debug or changing runtime.
	// BBR internal STARTUP/DRAIN/PROBE_BW/PROBE_RTT phases are not emitted by H1.
	timeline := []map[string]any{}
	slots := map[string]int{}
	for _, e := range after.Evidence {
		if e.Event != "sample" || e.Time.Before(begin) || e.Time.After(end) {
			continue
		}
		slot, ok := slots[e.Session]
		if !ok {
			slot = len(slots) + 1
			slots[e.Session] = slot
		}
		timeline = append(timeline, map[string]any{"offset_ms": e.Time.Sub(begin).Milliseconds(), "connection_ordinal": slot, "authenticated_age_ms": e.AuthenticatedAgeMS, "last_metrics_age_ms": e.LastMetricsAgeMS, "last_ack_age_ms": e.LastACKAgeMS, "smoothed_rtt_us": e.SmoothedRTTUS, "min_rtt_us": e.MinRTTUS, "cwnd_bytes": e.CongestionWindow, "bytes_in_flight": e.BytesInFlight, "ACK_total": e.AcknowledgedPackets, "declared_lost_total": e.LostPackets, "PTO_positive_updates_total": e.PTOPositiveUpdates})
	}
	result := map[string]any{"connections": connections, "variant": os.Getenv("PCS_H2_VARIANT"), "profile_enabled": os.Getenv("PCS_H1_SERVER_CPU_PROFILE") != "", "started_at": begin.UTC(), "completed_at": completed.UTC(), "finished_at": end.UTC(), "TCP_completion_seconds": completed.Sub(begin).Seconds(), "TCP_received_exact_bytes": received.Load(), "TCP_failures": failed.Load(), "small_request_count": len(latencies), "small_p50_ms": p50, "small_p95_ms": p95, "completion_CPU": delta(completionMetrics), "including_flush_CPU": delta(after), "harness_and_simulator_CPU_seconds": harnessCPU, "BBR_phase_observation": "unavailable; no runtime or debug changes", "sampled_connection_timeline": timeline, "up": h2LinkDelta(u0, up.snapshot()), "down": h2LinkDelta(d0, down.snapshot()), "up_scheduler_p95_us": up.lagP95(), "down_scheduler_p95_us": down.lagP95(), "evidence": h2EvidenceDelta(h2Totals(before.Evidence), h2Totals(after.Evidence)), "cgroup_before": cgBefore, "cgroup_after": cgAfter}
	body, _ := json.Marshal(result)
	t.Logf("H4_WARM_RESULT %s", body)
	if failed.Load() != 0 || received.Load() != (2<<20)+16*8192 || len(latencies) != 16 {
		t.Fatal("reliable payload mismatch")
	}
	if up.snapshot().SchedulerOverflow+down.snapshot().SchedulerOverflow != 0 {
		t.Fatal("bounded simulator overflow")
	}
}
