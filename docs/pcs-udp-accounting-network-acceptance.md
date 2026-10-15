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
