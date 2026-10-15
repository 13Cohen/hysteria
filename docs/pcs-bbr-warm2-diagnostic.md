# Four-connection warm 2 MiB control

The BBR pacer candidate remains **HOLD**. This independent H4 diagnostic restores
only the bulk total from 8 MiB to 2 MiB while retaining H3's complete warmup,
settling, topology and CPU accounting. The short four-connection regression
reappears: candidate goodput is **11.99% lower**, with **23.65% more server CPU
per delivered byte** at exact transfer completion. This rules out using H3's
warmer starting state alone to dismiss the original short-workload regression.
It still does not identify a specific internal BBR phase as the cause.

The pre-run plan was recorded before build or traffic. The same immutable H3
baseline and candidate server binaries were reused and their hashes checked.
Only the baseline client/harness was rebuilt to add the opt-in
`TestH4WarmTwoMiB` test; its protocol implementation is unchanged. The helper
is a deliberate copy of H3 with only opt-in/result names, a four-connection
restriction and the bulk-size constant changed. This keeps the frozen H3 test
and artifacts intact. No production runtime was rebuilt or deployed.

The shared artificial link remains 4 Mbps downstream, 2 Mbps upstream, 80 ms
propagation in each direction and a 64 KiB finite queue, without extra injected
loss or delay. Four outer QUIC connections share that bottleneck. Warmup stays
at a fixed total 256 KiB, divided sequentially across the connections, then
1.1 seconds settling and a fixed total 64 KiB concurrent resume. Each measured
run contains 2 MiB bulk plus sixteen 8 KiB small requests, divided equally across
the connections. The baseline client is fixed; only the server binary changes
in serial ABBA order.

| Run | Variant | Exact TCP bytes | Completion seconds | Server CPU at completion | Small-request p95 ms | Exact downstream queue drops |
|---|---|---:|---:|---:|---:|---:|
| 1 | Baseline | 2,228,224 | 5.0483 | 0.3861 | 2,102.4 | 1,093 |
| 2 | Candidate | 2,228,224 | 5.5198 | 0.4411 | 1,788.1 | 742 |
| 3 | Candidate | 2,228,224 | 5.8916 | 0.4807 | 1,659.9 | 852 |
| 4 | Baseline | 2,228,224 | 4.9953 | 0.3594 | 1,718.4 | 935 |

Mean run-p95 improves 9.76%, and exact queue drops decline from 2,028 to 1,594
across each variant's two runs. These gains do not waive the throughput or CPU
regression. Including the extra 1.1-second evidence flush changes the CPU/byte
increase only to 22.85%. At completion, mean user CPU changes from 0.2079 to
0.2852 seconds and system CPU from 0.1648 to 0.1757 seconds. Voluntary context
switches increase from 9,010 to 13,240. Unlike H3's one-connection case, this
short-workload CPU increase is mostly user CPU: the evidence does not support
attributing all candidate overhead to a single kernel or pacing-wakeup mechanism.

All payload checks passed. Every measured cgroup throttle delta is zero;
maximum simulator scheduling p95 is 2.488 ms and maximum memory peak is
26.14 MiB. These are within the same predeclared validity bounds. Limits remained
0.4 CPU, 512 MiB, GOMAXPROCS=1 and network isolation. The exact shared-lock windows
were build **18:45:07–18:45:27 UTC** and ABBA **18:45:50–18:46:42 UTC**, on
2026-10-15. Experiment containers were removed and the lock released afterward.

Together with H3, the same warm fixture shows a larger regression at 2 MiB than
at 8 MiB, consistent with a transfer-length-dependent cost or scheduling tradeoff.
Only two runs per variant were measured; BBR's internal phase is not emitted,
so startup, drain and recovery cannot be conclusively distinguished. Existing
anonymous H1 timelines are retained, but periodic counter snapshots are not
perfectly aligned with the exact TCP measurement boundaries. QUIC threshold
loss declarations are not physical loss rates or exact retransmission counts.
No additional mixed-UDP or profiling experiment was run.

[Original H2 matrix](pcs-bbr-network-acceptance.md) and
[H3 longer diagnostic](pcs-bbr-steady-diagnostic.md) are preserved. The original
16-run release hold remains, and the mixed UDP/TCP regression is still unresolved.
Per-run data, pre-run plan and reused-server hashes are in
[`results/bbr-warm2-20261015`](results/bbr-warm2-20261015).
