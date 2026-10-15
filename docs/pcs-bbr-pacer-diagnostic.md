# BBR pacing diagnostic and bounded evidence proposal

Status: isolated candidate, **not deployed; no network performance claim**.
The production observation remains on H1 `9baeab73ed0073add2fba7f837d9dae721ac8576`.
The QUIC dependency remains `v0.54.1-0.20250907230547-eb32f8aec5e2`.
BBR itself lives in Hysteria `core/internal/congestion/bbr`, not inside the
QUIC dependency. No dependency, congestion window, bandwidth setting, protocol,
authentication, padding, or production configuration changed here.

## Production evidence and what it does not establish

In the reported 2026-10-15 15:13:00–15:13:55 UTC interval, observed effective
congestion windows repeatedly reached 5,120–7,330 bytes at roughly 160–164 ms
RTT; loss declarations increased by 443 and packet acknowledgements by 2,361.
About 90% of the declarations had a time-threshold label. UDP target reads and
unique payload successfully enqueued both advanced approximately 1.71 MB, with
no observed send/target operation lasting at least one second. These are
server observations, not a receiver delivery count or a physical loss rate.

The 5,120-byte window is exactly the implementation's four times 1,280-byte
minimum. Its window/RTT scale is approximately 250–256 kbit/s, close to this
interval's payload rate; this is a useful consistency check, not an independent
proof of the limiting stage or a strict goodput bound. A sender can overshoot
the window by a packet, frames have overhead, and the reported rate is an
interval aggregate. A single effective window sample cannot distinguish:

* PROBE_RTT, which directly exposes the minimum window;
* loss recovery, which exposes the lesser of model and recovery windows;
* a small model window derived from the bandwidth estimate and min RTT.

The normal PROBE_RTT expiry is ten seconds, and its minimum holding time is
200 ms plus completion of a round. Its occurrence alone is not an error.
Likewise, an enqueue that returns promptly does not establish delivery or an
empty transport queue. The inner QUIC source may slow its offering after loss,
leaving the outer sender without a long application wait.

Relevant fixed-source locations: `bbr_sender.go:34,393–403,583–609,742–789,869–931`.

## Confirmed pacing implementation difference

In the production basis, the pacer is constructed with `bandwidthForPacer`
(`bbr_sender.go:296`). That callback uses the bandwidth estimate multiplied by
the **congestion window gain**, while `calculatePacingRate` separately computes
the intended rate using the **pacing gain** (`:555–563,829–866`). The calculated
rate therefore does not directly drive the real token bucket. The PROBE_BW
1.25/0.75/1 cycle and DRAIN gain can change without the pacer following them.

This is also addressed by the official upstream commit
[4bf55a99ede37ee6112833ec234914010ab6337d](https://github.com/apernet/hysteria/commit/4bf55a99ede37ee6112833ec234914010ab6337d)
(2025-12-30, `fix: logic issues with BBR impl`). The current official source,
pinned during this investigation to
[ca8fbd874fd6ca82413948de6dc5924405e103e1](https://github.com/apernet/hysteria/blob/ca8fbd874fd6ca82413948de6dc5924405e103e1/core/internal/congestion/bbr/bbr_sender.go#L663),
still uses `PacingRate()` in this callback. The same upstream fix also corrects
ACK aggregation accounting. That other change, newer profiles, and MTU window
rescaling are intentionally separate candidates, not part of this backport.

Candidate `e99e90c` changes only the callback expression and the existing floor's
unit comment. The floor is 65,536 **bytes/s**, or 524,288 bit/s; this is a pacing
floor, not a guaranteed application throughput floor. It remains unchanged.
The source and unit correction match the two relevant upstream hunks.

### Actual fake-clock execution

Diagnostic `bafdfef` uses the real sender and real token bucket with a fake
clock, a fixed estimate of 100,000 bytes/s, and a window gain of two. It
exhausts the real initial ten-packet allowance, then checks the next send time,
checks that one nanosecond before it has insufficient budget, and checks that
the budget is sufficient at the calculated time. It does not sleep, open a
socket, run a goroutine, or simulate network delivery.

| State | Calculated bytes/s | Baseline executed bytes/s | Candidate executed bytes/s | Baseline next 1,280 B | Candidate next 1,280 B |
| --- | ---: | ---: | ---: | ---: | ---: |
| PROBE_BW high | 125,000 | 200,000 | 125,000 | 6.400 ms | 10.240 ms |
| PROBE_BW low | 75,000 | 200,000 | 75,000 | 6.400 ms | 17.067 ms |
| PROBE_BW cruise | 100,000 | 200,000 | 100,000 | 6.400 ms | 12.800 ms |
| DRAIN | 34,662 | 200,000 | 65,536, floor applies | 6.400 ms | 19.531 ms |

The default baseline diagnostic passes and reports the difference. With
`PCS_REQUIRE_CALCULATED_PACING=1`, baseline fails all four calculated-rate
gates and the candidate passes them. The default floor-unit test also passes.
This validates callback wiring and execution, not complete BBR correctness or
full state transition behavior. It does not prove less loss or better video.

Commands inside the existing Go 1.25.6 builder:

```sh
cd core
go test -mod=readonly -p 1 ./internal/congestion/bbr -run 'TestPacer.*Diagnostic' -count=1 -timeout=10s -v
PCS_REQUIRE_CALCULATED_PACING=1 go test -mod=readonly -p 1 ./internal/congestion/bbr -run 'TestPacer.*Diagnostic' -count=1 -timeout=10s -v
```

CPU windows: **15:22:29–15:22:41** and **15:23:23–15:23:29 UTC**.
The shared experiment lock was held only during each window. Each container
had 0.25 CPU, 384 MiB RAM, 768 MiB RAM+swap, GOMAXPROCS=1, and `--network none`.
Reported package times were 0.006 s baseline diagnostic, 0.007 s expected
baseline gate failure, and 0.028 s candidate gate success. Container startup
and cached compilation are included in the wall-clock windows. No production
service was restarted. See [sanitized results](results/20261015-bbr-pacer/results.json).

## Loss interpretation and separate source concerns

In fixed quic-go `sent_packet_handler.go:686–751`, a packet no greater than the
largest acknowledged number is declared lost after
`9/8 * max(latestRTT, smoothedRTT)`, at least the timer granularity. At the
observed RTT this is roughly 180–184.5 ms, though latest RTT can be higher.
The time condition is tested **before** the three-packet threshold. Thus a
time-threshold label does not distinguish physical loss, packet reordering,
reverse-path loss, or peer ACK behavior; packets satisfying both conditions
receive the time label. A delayed or compressed ACK can also distort sampling.

DATAGRAM frames count toward in-flight bytes and congestion control, despite
having no retransmission handler (`packet_packer.go:656–667` and
`sent_packet_handler.go:284–293`). Declared loss can reduce the recovery window,
while the inner QUIC application must recover missing payload itself.

The existing `AcknowledgedPacket` callback does **not** close the late-ACK
evidence gap. `sentPacketHistory.DeclareLost` removes the packet from history
(`sent_packet_history.go:214–230`), so a later ACK range covering that packet
does not produce the same ordinary acknowledged callback. Lost plus acked is
therefore not a reliable denominator for real wire loss, and absence of a late
callback does not prove a packet never reached the client.

Two further issues remain isolated from the pacing patch:

1. `OnPacketSent` in quic-go receives in-flight bytes **after** adding the
   current packet, and its extension adapter passes this through unchanged.
   Hysteria's sender tests this for zero to detect quiescence, while its sampler
   also records `bytesInFlight + bytes` (`bbr_sender.go:338–342`,
   `bandwidth_sampler.go:574–594`). The boundary semantics deserve a separate
   actual-adapter regression; data packets after idle cannot take the zero
   branch through this call, although ACK-only packets can. No correction or
   performance claim is included here.
2. `maybeAppLimited` infers application limitation solely from in-flight bytes
   below a target window (`bbr_sender.go:717–720`). It does not inspect the
   DATAGRAM send queue or actual application backlog. Its flag is an algorithm
   heuristic, not proof that the video source has no more data.

The previously documented MTU admission/packer-discard hypothesis also stays
separate. A frame discarded by the packer never becomes an on-wire packet
loss declaration. Raising frame sizes without this boundary test could create
a different delivery failure; unchanged packet caps are a control for this
pacing experiment.

## Low-cost complete BBR evidence design, not implemented

Read mutable sender fields only on the transport owner path. Do not let the
monitor goroutine read them directly, and do not add a global lock, per-packet
log record, timer goroutine, or a blocking sink.

Add an optional nil-by-default observer to the existing BBR constructor/helper.
At the end of `OnCongestionEventEx`, use its existing `eventTime` to publish
one immutable scalar snapshot at most once per second. Record small transition
counters/dwell accumulators on the same owner path so short PROBE_RTT/recovery
events are not lost between snapshots. An existing evidence state receives a
copied snapshot; its normal five-second writer exports it. Bound the number
of observed authenticated connections through the existing manager cap, and
export timestamp/age and an explicit absent state when no event arrived.

Required fields and units:

| Group | Fields |
| --- | --- |
| Window explanation | `mode`, `recovery_state`, `model_cwnd_B`, `recovery_window_B`, `effective_cwnd_B`, `min_cwnd_B`, `target_cwnd_B`, `bytes_in_flight_B`, `round_count` |
| Model and sample | `bandwidth_estimate_bps`, `last_sample_app_limited`, `sampler_app_limited`, `has_non_app_limited_sample`, `full_bandwidth_reached` |
| Actual pacing | `pacing_gain`, `cwnd_gain`, `calculated_pacing_Bps`, `actual_pacer_Bps`, `pacer_floor_applied`, `max_datagram_size_B` |
| RTT/state duration | `bbr_min_rtt_us`, `min_rtt_age_us`, `min_rtt_expired_events`, `probe_rtt_entries/exits`, `probe_rtt_dwell_us`, `recovery_entries/exits`, `recovery_dwell_us` |
| Validity | `snapshot_age_ms`, `observed_duration_ms`, observer saturation/dropped snapshots, close reason and a final copied state |

Capture the expiry boolean/counter when computed, before it is replaced by a
new min-RTT timestamp. Distinguish BBR's sampled RTT from transport smoothed
RTT; the sampler's RTT does not subtract peer ACK delay. Label application
limitation as the implementation's inference. Calculated and executed rates
must both retain units, including the floor, so a debug print cannot masquerade
as the actual pacer input.

If a send-limitation counter is added, record that `CanSend` rejected a send
and that a pacing check rejected a send separately. Counts are not elapsed
time unless transitions are timestamped on an existing time-bearing path;
avoid adding `time.Now()` to every eligibility check. Queue occupancy and
backlog are a separate bounded transport snapshot and remain necessary to
distinguish cwnd-limited from supply-limited traffic.

Observer acceptance: default-disabled cost comparison; all three minimum-CWND
causes represented; short transitions retained; no mutation/race from readers;
nil observer behavior identical; stable schema units; authenticated-only
bounded retention; explicit stale/evicted coverage; no addresses, domains,
payload, credentials, raw packet numbers or connection IDs in exports.

For ACK ambiguity, a separate opt-in 30-second diagnostic can retain a bounded
recently-declared-lost packet-number set **internally**, then inspect incoming
ACK ranges in the receive tracer, before history filtering. Count late-covered
declarations, ACK gap/aggregation and range truncation; export only aggregate
counts and timing buckets. Bound connections, ranges examined, retained items,
TTL, CPU and output, and report every coverage gap. A late covering ACK is
positive evidence against permanent forward loss; no such ACK remains
ambiguous. Existing ordinary acknowledged callbacks cannot substitute for it.

## Next controlled network acceptance, not run

Keep this one-line pacing change independent of the adapter, ACK aggregation,
window, queue, MTU and Brutal changes. First confirm startup/no-estimate and
minimum-rate edge cases, recovery/PROBE_RTT transitions, and total suite/race
behavior before a network candidate. Then run serial baseline/candidate pairs
with identical synthetic payload sequence, offered rate, route, MTU and random
seed. Use an isolated server/client pair and namespace-local impairment only,
never host/production shaping. Keep load and duration bounded.

The minimum useful matrix has:

1. No impairment at 160 ms RTT and a fixed bottleneck, at both sparse offered
   load and saturated load. This separates source limitation from a window or
   pacing defect. Add a low-RTT control.
2. Forward loss with exact injected packet IDs/count, then an independent
   reorder/delay-only condition with **zero injected loss**. Include delayed
   packets beyond the time-loss threshold. Record actual client receipt and
   complete unique payload rather than treating sender declarations as truth.
3. Reverse ACK-path delay/loss separately, including burst/compressed ACK
   release. Keep forward delivery intact to test apparent loss and sampling.
4. A fixed on/off offered-load pattern, then a controlled inner HTTP/3/QUIC
   producer versus plain sequenced UDP at the same payload and mean rate.
   Correlate inner supply, outer queue, observer state and client delivery.
5. Two UDP sessions sharing one outer connection, then two outer connections,
   with one disturbed flow and an undisturbed control. Measure fairness and
   healthy-flow latency, not only aggregate throughput.

Retain current DATAGRAM caps for these pairs; add independent near-limit MTU
tests only afterward. Record unique delivered goodput, payload integrity,
reordering, queue/drop counters, late-ACK coverage, effective/model/recovery
CWND, calculated/actual pacing, probe/recovery dwell, CPU and RSS. Compare
repeated pairs with the same offered workload, not different video segments.
Stop on connection failures, receiver corruption, major latency/fairness
regression or resource-cap saturation. A local impairment fixture validates
mechanisms; it cannot identify physical loss on the real client route.

For the existing real route, a server-only observer can narrow the cause but
cannot always distinguish forward data loss from reverse ACK loss or client
processing. A controlled receiver's sequence/delivery report or client-side
trace is required for that final separation. Do not label a lack of server
errors as end-to-end success, and do not raise Brutal bandwidth to conceal an
unmeasured queue or congestion effect.
