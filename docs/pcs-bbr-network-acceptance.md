# Server BBR pacer: controlled network acceptance

This branch tests the isolated server-only pacer correction `e99e90c` against
the production source `9baeab7`. It does not change the production service.
The existing baseline client binary is used in every run. The candidate server
differs only in the previously reviewed pacer expression and its units comment;
the test helpers and all other server behavior are identical.

The test uses actual TLS/QUIC connections and TCP/UDP targets inside a Docker
`--network none` namespace. A userspace UDP link supplies 80 ms propagation per
direction, 4 Mbit/s downstream and 2 Mbit/s upstream, a 64 KiB serialization
queue in each direction, and 28 bytes of simulated IPv4/UDP overhead per packet.
This is a controlled bottleneck experiment, not a measurement of Internet speed.
Finite queues can drop packets in both profiles. `clean_tcp` means no additional
deterministic loss or delay injection, not a loss-free path.

Each scenario uses an ABBA sequence with two baseline and two candidate runs.
There are one and four independent QUIC connections. The total reliable payload
is fixed at 2 MiB bulk plus sixteen 8 KiB small requests. Mixed scenarios also
offer exactly 3,600 UDP payloads of 1,350 bytes over twelve seconds, with two UDP
sessions per QUIC connection. The client checks the lane, sequence and entire
payload. Delivery is counted at the client, rather than treating server enqueue
as delivery. The 1,350-byte packets exercise the unchanged fragmentation path.

The mixed profile checks arrival-index modulo 200 for injected loss and modulo
97 for an extra 40 ms delay, only after finite-queue admission. The arrival index
includes queue-dropped packets. The original frozen plan described this as an
admitted-packet index; `gates-errata.json` preserves that correction without
changing the algorithm, thresholds, or existing runs. Do not infer an exact
0.5% admitted-packet loss rate from the configuration.

Acceptance requires exact reliable data, no accepted corrupt, duplicate or
cross-session UDP payloads, unchanged trust/name/authentication rejection, and
no unexpected server exit or hang. Each scenario must retain at least 95% of
baseline TCP and UDP goodput, no more than a two percentage-point UDP delivery
decline, no more than 20% growth in mean per-run small-request p95, and no more
than 20% growth in server CPU per received payload byte. At least one congested
scenario must improve TCP goodput by 10% or reduce exact injected/queue drop
decisions per received byte by 20%. Failing any applicable non-regression gate
holds the candidate even if another metric improves. Sixteen small requests per
run are insufficient to establish a production tail-latency distribution.

All heavy commands hold `/run/lock/pcs-proxy-experiment.lock`. Containers have
`GOMAXPROCS=1`, 0.4 CPU, 512 MiB memory and a 768 MiB memory-plus-swap cap.
The server runs as a separate process, and its CPU is measured separately. The
client, target helpers and link simulator share the harness process; their
combined CPU is an upper bound for simulator CPU, not a separate simulator-only
measurement. Cgroup CPU usage, throttling, memory and link scheduler delay are
recorded. Scheduler p95 lateness above 5 ms or throttling above 10% of measured
wall time makes a run inconclusive. UTC run and lock boundaries identify the
intervals that may interfere with natural production observations.

Loss counters have distinct meanings. Simulator queue and injected drops are
exact decisions made by the fixture. QUIC loss is a threshold-based declaration
and is not a physical path-loss rate. Exact TCP completion validates recovery;
QUIC DATAGRAM data is not retransmitted. This experiment does not have an exact
STREAM retransmission counter and does not label excess wire bytes as
retransmissions. A few warmup packets may cross the measurement start, so
submitted-minus-forwarded is not used as a drop counter. The warmup TCP exchange
duration is not an RTT measurement. Latest RTT gauges are taken from H1 evidence.
The simulator drop counters do not cover the target-to-server UDP socket or
application queues. Under pacing backpressure, target-offered bytes can exceed
H1 target-read and enqueue bytes within the fixed receive window. An absence of
send errors does not make those bytes delivered. Client-validated received bytes
remain the primary UDP metric, and only that metric covers the full test path.

Before network tests, the boundary regression verifies startup without a
bandwidth estimate, startup without RTT, PROBE_RTT, both recovery states and the
minimum pacing floor. The test checks finite pacing progress and unchanged
minimum-window behavior. It does not claim to fix the separate inflight-accounting
question. Those boundary cases passed with the race detector.

The opt-in entry point is `app/cmd/TestH2ControlledNetwork` with
`PCS_H2_NETWORK=1`, `PCS_H2_PROFILE=clean_tcp|loss_reorder_mixed`, and
`PCS_H2_CONNECTIONS=1|4`. `PCS_H1_SERVER_BINARY` selects the server binary while
the baseline test binary always owns the client. Build with the fixed Go 1.25.6
toolchain and unchanged module versions. Published results contain numeric
aggregates and hashes; raw H1 events remain in private evidence storage.

## Interim result, 2026-10-15

The first eight official runs completed. The one-connection TCP-only group is
**HOLD**: aggregate TCP goodput improved 24.79%, mean per-run small-request p95
fell 52.35%, but server CPU per received byte rose 28.41%, exceeding the frozen
20% ceiling. The measurement itself was valid: no measured quota throttling,
scheduler p95 at most 2.586 ms, and exact reliable payload in all four runs.

The one-connection mixed group passed its gates: TCP goodput improved 41.54%,
mean per-run small-request p95 fell 71.42%, and actual UDP delivery increased
from 41.694% to 78.194% of the same 3,600 offered payloads per run. Server CPU per
actually delivered byte rose 4.92%. This is a result in the defined simulation,
not a production speed claim. The candidate's target-read and enqueue counters
did not reach all target-offered bytes within the fixed receive window; those
unread or pending bytes are not silently counted as simulator drops or delivery.

The overall candidate remains **HOLD** because the TCP-only CPU gate failed.
Four-connection groups are still pending. Numeric per-run records and the frozen
plan are in [`results.json`](results/bbr-network-20261015/results.json), with
separate lock windows for alignment with production observations. The independent
UDP accounting candidate was accepted on its own branch, retaining the original
pacer; that result does not override this pacer hold.
