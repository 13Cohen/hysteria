# Longer BBR pacer diagnostic

This is a separate diagnostic following the original 16-run network matrix.
That matrix remains **HOLD**: clean one-connection CPU per delivered byte rose
28.41%, clean four-connection TCP goodput fell 10.20%, and mixed four-connection
TCP goodput fell 15.72%. Its gates and raw results are retained in
[the original acceptance record](pcs-bbr-network-acceptance.md). A longer test
cannot retroactively turn those regressions into a release pass.

Before building or running this diagnostic, the plan was extended from one
connection to both one and four outer QUIC connections. Each group uses serial
ABBA order, the same baseline client, and changes only the server pacer. The
fixed measured total is 8 MiB of bulk TCP plus sixteen 8 KiB requests. Bulk and
small requests are divided evenly across the connections, following the
original matrix's allocation rule. The connections share the same bounded
4 Mbps downstream / 2 Mbps upstream link, 80 ms propagation in each direction,
and 64 KiB serialization queue. There is no extra deterministic impairment,
but the finite queue can still drop packets.

Warmup is excluded from CPU measurement: a fixed total 256 KiB divided across
connections, 1.1 seconds of settling, then a fixed total 64 KiB resume transfer
split concurrently across the connections. The measurement has two explicit
end points: immediately after all exact TCP payloads complete, and after a
further 1.1 seconds for evidence logging. Separate server-process RUSAGE reports
user/system CPU, total CPU and context switches at both points. Context switches
are scheduler counters, not exact pacer wakeups. The client and simulator remain
in a different process from the measured server, in the same limited container.

The test keeps H1 evidence enabled with a 1-second interval, matching the
original network fixture. Production uses a 5-second interval; that difference,
the artificial bottleneck and two runs per variant limit generalization.
Validity still requires exact payloads, simulator scheduling p95 no more than
5 ms and cgroup throttling no more than 10% of measurement wall time. Each run
uses at most 0.4 CPU and 512 MiB, and all builds/load take the shared experiment
lock. CPU profiles were deferred to prioritize both connection-count groups.

The eight runs completed with exact payloads and valid simulation measurements.
These experiments ran in isolated containers with no production network
changes or production BBR rollout.

## Source interpretation before the diagnostic

The constructor sets the congestion-window startup gain to 2.0, while the
startup pacing gain is 2.885. The original pacer uses estimated bandwidth times
the congestion-window gain. The candidate instead uses `PacingRate()`, which
follows startup pacing, the approximately 0.347 drain gain, and the PROBE_BW
1.25/0.75/1 gain cycle. It uses the pre-existing `PacingRate()` initial-window/RTT fallback; the old
pacer instead reaches the 64 KiB/s floor when the bandwidth estimate is zero.
Both retain the minimum pacing floor. This is a substantial scheduling change, not merely a
renamed multiplier. The short original four-connection workload sends only
512 KiB of bulk data per connection, so startup and draining may occupy a large
share of its lifetime; this is a hypothesis, not a measured phase attribution.

The original clean four-connection runs already show the tradeoff: exact
simulator downstream queue drops fell from 868/807 to 566/586, while completion
slowed from 5.019/5.023 seconds to 5.617/5.566 seconds. Fewer queue drops alone did
not meet the speed gate. Existing H1 samples expose RTT, cwnd, inflight and
ACK/loss/PTO progress but no internal BBR phase. The phase tracer hooks in this
pinned BBR implementation are commented out. The diagnostic preserves a bounded
anonymous timeline of the existing samples after measurement; it does not turn
on extra BBR debug output or add runtime instrumentation to infer the phase.

The opt-in test is `TestH3SteadyPacerCPU` in `app/cmd`. Build the same test helper
for both runtime variants, then always launch the baseline test binary as the
client/harness with `PCS_H3_STEADY=1` and `PCS_H3_CONNECTIONS=1` or `4`.
`PCS_H1_SERVER_BINARY` selects only the helper server binary and
`PCS_H2_VARIANT` labels the run. `H3_STEADY_RESULT` is the bounded numeric result;
raw helper logs remain private. The test is skipped unless explicitly enabled.


## Completed results

| Outer connections | TCP goodput change | Mean run small-request p95 change | CPU/byte at completion | CPU/byte including flush |
|---|---:|---:|---:|---:|
| 1 | +6.35% | -63.91% | +17.18% | +17.27% |
| 4 | -0.80% | -72.63% | +10.19% | +10.02% |

All eight runs delivered exactly 8,519,680 bytes. The one-connection completion
pairs were baseline 19.886/19.411 seconds versus candidate 18.546/18.403;
four-connection pairs were 19.087/18.581 versus 18.355/19.617. The latter pair
has enough variation that its nominal -0.80% should not be treated as a proven
population effect. Small-request p95 is computed from only sixteen requests per
run, and the table compares means of two run-p95 values, not a pooled percentile.

At one connection, mean server CPU was 1.211 seconds versus 1.419 seconds per
8.125 MiB. User CPU barely changed (0.803 to 0.817 seconds), while system CPU
rose from 0.408 to 0.602 seconds. Voluntary context switches rose from 23,744 to
50,872. At four connections, server CPU rose from 1.409 to 1.552 seconds, user
CPU from 0.828 to 0.919, system CPU from 0.580 to 0.633, and voluntary context
switches from 39,146 to 54,333. The additional 1.1-second flush used approximately
3–12 ms CPU per run: it does not explain most of the candidate's extra CPU.
This supports a scheduling/system-call cost hypothesis, without measuring
exact pacer wakeups or attributing specific kernel functions. Observed server
CPU utilization stayed roughly 6–8% of one core at this 4 Mbps bottleneck; this
is not a maximum-capacity measurement.

Exact simulated downstream queue drops decreased from 5,114/5,248 to 154/192
at one connection, and 5,062/4,757 to 2,005/1,674 at four. The candidate still
sent every required TCP byte correctly. QUIC loss declarations and wire-byte
excess are not labeled as physical loss or exact STREAM retransmissions.

Maximum scheduler p95 was 2.416 ms and maximum cgroup memory peak was
26.59 MiB. Seven runs had zero CPU throttling; one four-connection candidate
run had 5.779 ms (less than 0.03% of measured wall time), retained in the data.
The fixed validity thresholds passed for all runs. Existing H1 sample timelines
are preserved with run-local anonymous connection ordinals. They have no BBR
phase field, are best-effort atomic snapshots, and their cumulative-counter
sample boundaries can differ from exact payload measurement boundaries.

The original 16-run **HOLD remains in force**. H3 changed both measured length
and premeasurement warmup relative to H2, so it cannot isolate length alone or
prove the old four-connection regression was merely startup. The mixed UDP/TCP
regression has not been removed. A separately planned H4 diagnostic keeps this
warmup and restores only the four-connection bulk total to 2 MiB; its results
will be kept separate. No BBR production rollout is authorized by these data.

Exclusive host-resource windows (UTC): build 18:36:37–18:37:11; one-connection
ABBA 18:37:18–18:38:54; four-connection ABBA 18:38:59–18:40:44. Production
observation must exclude these intervals when estimating natural resource use.
The resource-limited experiment containers were removed after completion.

Numeric per-run results, the pre-run plan and its explicitly recorded amendment,
and binary/source hashes are in
[`results/bbr-steady-20261015`](results/bbr-steady-20261015).
