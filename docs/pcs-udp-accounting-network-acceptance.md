# UDP accounting: real-network acceptance

This branch keeps the production BBR pacer and tests the independent accounting
fix `5140d46`. It must not be treated as acceptance of the separate pacer change.
The fix charges one original UDP payload before any fragmentation attempt.
Fragments skip repeated charging, while the original logger refusal still stops
the connection before fragmentation or enqueue. This remains an attempted-payload
accounting API; it is not an end-to-end delivery counter.

`TestH2AccountingNetwork` creates real authenticated TLS/QUIC clients with a
controlled 80 ms delay in each direction and finite 4/2 Mbit/s link queues.
One- and four-connection cases send and receive exact 1,000, 1,350 and 3,000-byte
payloads. For each connection the client receives 5,350 bytes; H1 target-read
and enqueue counters must agree. The unchanged baseline API records 9,700
download bytes because it charges the two larger original payloads again during
fragmentation. The candidate must record exactly 5,350, with the same exact
received payloads and the same two fragmented original messages. Upload counts
must remain 5,350. No production destinations or accounts are used.

`TestH2AccountingFragmentDenial` waits until a synthetic target has received the
full 1,350-byte upload, marks that synthetic authenticated identity for refusal
through the test-local management endpoint, then releases the target reply.
The client must be closed without receiving the reply. Download accounting,
fragment attempts and enqueued payload remain zero, while H1 records that the
target produced the original 1,350 bytes. This verifies actual connection and
fragment-path behavior rather than only mocking `LogTraffic`.

The existing H1 security test separately checks invalid certificate trust,
incorrect server name and incorrect authentication. Test-only management routes
bind inside the isolated namespace and use synthetic credentials. No public
management endpoint is added to the application.

All compile and test commands hold the shared experiment lock. Tests use Docker
`--network none`, Go 1.25.6, unchanged module versions, `GOMAXPROCS=1`, 0.4 CPU
and 512 MiB memory. The opt-in environment is `PCS_H2_ACCOUNTING=1`;
`PCS_H1_SERVER_BINARY` selects the baseline or accounting candidate server.
`PCS_H2_EXPECT_ACCOUNTING=duplicate_baseline` deliberately verifies the old
duplicate count, rather than interpreting the known old result as a test error.
No network speed gain is claimed from this accounting-only change.

Acceptance passed on 2026-10-15. The complete locked build/test interval was
17:38:02–17:38:51 UTC. Both one- and four-connection baseline and candidate cases
passed, as did the real fragmented-reply refusal and TLS/name/authentication
rejections. Numeric results, source hashes and binary hashes are retained in
[`results.json`](results/udp-accounting-network-20261015/results.json).
The candidate keeps the original BBR pacer; it is suitable for a separately
reviewed accounting-only release and subsequent production observation.

## Production observation of the accounting-only release

The immutable `224825dbc5e0dd01e55c27785d67caeb6e72a969` source was deployed as
`pcsproxy2` at 2026-10-15 17:50:20.248322 UTC. The original BBR pacer remained in
place; the experimental pacer was not included. A temporary client using the
same CLI image verified the real certificate chain and server name without
skipping verification, fetched an HTTPS response (200, 577 body bytes), and
validated one UDP DNS response (29 request bytes, 61 response bytes). Its
traffic generation window was 17:52:49.874215–17:52:51.545003 UTC. Its complete
server-side connection lasted until 17:53:21.984500 UTC and ended with an idle
timeout after client cleanup. The entire synthetic connection is excluded from
natural-user observations.

The exact first 30-minute window, 17:50:20.248322–18:20:20.248322 UTC, contains
180 collector samples, a maximum sampling gap of 10.006 seconds, no collector
errors, no counter resets, and no container restart. Only the synthetic
connection was observed: **there were no natural authenticated user sessions**.
API counters increased by 1,333 upload bytes and 9,015 download bytes, including
TLS and the synthetic checks. Kernel UDP error counters and socket drops stayed
at zero. These idle observations cannot estimate a user fault rate or prove a
video-speed improvement.

The process used 0.53 CPU seconds across the 1,790.048 seconds covered by samples.
RSS median was 20.79 MiB and maximum 22.00 MiB. Compilation, isolated experiments,
synthetic activity and pre-release validation on the shared host are explicitly
recorded as excluded intervals. Adjacent samples wholly within the remaining
segments cover 600.015 seconds and 0.16 process CPU seconds (0.0267% of one core),
with zero application traffic. Counter differences never bridge an excluded
interval. Some bounds deliberately include setup/cleanup and are labeled
conservative, rather than represented as exact benchmark bounds.

The bounded, sanitized evidence is in
[`results/udp-accounting-postdeploy-20261015/30m.json`](results/udp-accounting-postdeploy-20261015/30m.json).
It contains no client addresses, credentials, target addresses or session IDs.
The corrected API counts each original attempted payload once; it remains an
attempt counter, not proof of client delivery.
