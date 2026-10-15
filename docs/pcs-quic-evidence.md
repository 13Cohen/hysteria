# PCS H1: bounded QUIC evidence candidate

This branch starts at the deployed Hysteria v2.6.4 commit `2146852c483b7e4f68f405023034b8241bfc49c6`. It keeps the existing `github.com/apernet/quic-go v0.54.1-0.20250907230547-eb32f8aec5e2` dependency. The repository is maintained separately from PCS deployment configuration. The upstream repository is now `HyNetworks/hysteria`; historical module paths and dependency versions are unchanged.

**Candidate only: not approved for production rollout yet.** This change adds evidence, not a new congestion controller or a claimed speed improvement. H2 will evaluate cancellation-aware dialing and bounded fallback across all resolved addresses separately.

## Enable explicitly

The default is off. A candidate config can add:

```yaml
quicEvidence:
  enabled: true
  interval: 5s
  maxConnections: 128
  streamEvidence: true
  udpEvidence: true
  packetEvidence: false
  maxStreams: 256
  maxStreamsPerConnection: 32
```

The interval is limited to 1–60 seconds; the registry to 1–512 connection slots. Defaults are 5 seconds and 128 slots. Slots include unauthenticated QUIC handshakes so a handshake flood cannot create an unbounded evidence registry. If no slot is available the connection proceeds without evidence; its transport is not rejected or altered. `untracked_connections_total` reports that coverage loss in subsequent authenticated-session records; if no tracked session authenticates, H1 emits no standalone server health record. The original TLS/authentication, masquerade, QUIC windows, idle timeout, BBR selection, and `DisablePathManager=true` port-hopping behavior remain unchanged.

## What is recorded

Only successfully authenticated sessions emit `quic_evidence` records. They have a fresh random session namespace per server instance and an internal tracing sequence; they do not expose on-wire QUIC connection IDs. `stats_connection` links to the existing traffic API's randomly assigned connection number, but that 32-bit number alone is not a globally unique identifier. Repeated authentication on the same QUIC connection does not allocate a new session or emit a second authentication event.

Connection event types are `authenticated`, periodic `sample`, and `closed`. Optional application evidence adds `stream_sample`, selected `stream_closed`, `stream_terminal_update`, `stream_operation_slow_complete`, and `udp_operation_slow_complete`. Each record contains schema version, capture time, session age, optional packet/byte totals (null unless packetEvidence is enabled), acknowledged packet count, declared loss counts by reason, current/maximum PTO count and positive PTO-count callback updates, RTT gauges, congestion window, bytes/packets in flight, MTU, and ages of the most recent observed receive/send/ACK/metrics event. Close classification includes only a fixed category, numerical protocol code, and local/remote flag where meaningful. Raw error messages are excluded.

`pto_positive_updates` counts calls to `UpdatedPTOCount(n)` where `n > 0`; it is deliberately not named total timer firings. In the pinned dependency's `internal/ackhandler/sent_packet_handler.go`, the positive callback follows the normal PTO timer branch; other call sites reset it to zero. The unvalidated-address special branch increments PTO without the same callback, so the callback is not a universal count of every internal timer path. Authentication gating further excludes pre-authentication activity.

The callback semantics above were checked against the pinned dependency: [PTO update call sites](https://github.com/apernet/quic-go/blob/eb32f8aec5e2/internal/ackhandler/sent_packet_handler.go), [connection context and tracer association](https://github.com/apernet/quic-go/blob/eb32f8aec5e2/server.go), and [packet callback frame construction](https://github.com/apernet/quic-go/blob/eb32f8aec5e2/connection.go).

No client/server addresses, domains, request targets, authentication IDs, passwords, payloads, packet numbers, token contents, or frame contents are exported. Existing upstream logs are unaffected and retain their prior behavior. Authentication failures and unauthenticated closes produce no new session records.

Counters start after authentication and describe callback observation time; a later ACK can refer to a packet sent before authentication. Packet byte counts include protocol overhead and are not application delivery counters. A packet declared lost can later be acknowledged; declared-loss and ACK counts must not be interpreted as mutually exclusive outcomes or direct path loss measurements. Gauge fields are independently sampled atomics, not a consistent lock-held snapshot of the transport. `-1` age means no callback has yet supplied that observation; zero-valued RTT/window/MTU fields may likewise be unobserved. The gauge-age field must accompany stale measurements.

An idle timeout is a transport close reason, not by itself a confirmed user failure. Pending application work, useful-byte progress and blocked writes require application-level evidence and controlled end-to-end probes. The optional application evidence below distinguishes pending copy operations, but does not split DNS/dial attempts or provide a complete request denominator. Do not infer users from shared password identities or infer device reconnection from a new session alone.

## Bounds and overhead

Packet callbacks only update fixed-size atomics; they do not take the registry mutex, write logs, call a sink, retain frame slices or RTT pointers, or create worker goroutines. Connection setup/authentication/close use the short-lived registry mutex. Periodic snapshots run in one background worker per server instance. A 64-entry lifecycle-event queue drops records rather than blocking. A token bucket per server instance replenishes 64 records/second, with a bounded burst of `maxConnections + 64`, so a normal periodic snapshot batch fits while sustained floods remain limited. `dropped_events_total` is cumulative and includes both bounds. It must be checked before using logs as a complete population.

Logger backpressure can block that one worker, but not packet callbacks or proxy shutdown. At most one emitter goroutine **per server instance** can remain blocked until the destination resumes. Repeated `NewServer`/`Close` against a permanently blocked sink can retain one worker per old instance because the logger interface has no cancellation primitive; the bound is not a process-wide no-leak guarantee. The CLI normally creates one server instance and exits on a fatal serving error. The repeated-instance test verifies those workers exit when the sink resumes. Shutdown is best effort and can discard queued final records. A snapshot already taken while a connection is closing can race with its final event; order by capture time and event type, not output position alone. Evidence may be incomplete during slow logging, saturation or shutdown.

Full-packet mode adds transport overhead: explicitly enabling the upstream receive/send packet callbacks causes quic-go to assemble frame metadata, even though H1 does not persist it. Callback-only allocation/latency benchmarks cannot establish complete end-to-end cost. Before production, compare tracing off/on under the same bounded authenticated flow mix, inspect CPU/allocations/GC and p95/p99 latency, validate no sensitive fields escape, and confirm counters against known injected ACK/loss/idle sequences. No throughput benefit is claimed by adding observability.

## Why no quic-go fork yet

The pinned dependency already provides `Config.Tracer` and `logging.ConnectionTracer` callbacks for the measurements used here. Hysteria owns its BBR implementation and can extend that state reporting separately. Precise internal path-switch decisions, send-queue scheduling or flow-control blocking points would need a later API extension or dependency fork if existing evidence proves them necessary. Merely switching `DisablePathManager` off would alter the deployed port-hopping path and is outside H1.

## Candidate validation

Targeted tests cover authentication gating, missing-observation sentinels, counter aggregation/PTO resets, typed close classifications without error-string leakage, slot reuse/caps, bounded queue/rate, blocked logger isolation including repeated instances, concurrent snapshots, and application configuration parsing. A real local QUIC client test rejects bad authentication, echoes payloads, checks authenticated ACK/RTT/window records and the close event. It follows upstream fixture tests in skipping certificate verification and is not a certificate-policy acceptance test. Run race tests and callback benchmarks under the host's shared resource lock. Existing core integration tests are required before any rollout to check authentication, transfer and timeout behavior with diagnostics disabled. A controlled live tracing off/on overhead comparison remains a separate acceptance gate.

Only push the `pcs/quic-evidence` branch. Upstream release workflows react to `app/v*` tags and master-specific push triggers; do not push release tags or update master while validating this candidate. PCS image/source pins remain unchanged until a separately reviewed rollout.


## Initial H1 validation recorded on 2026-10-15

The candidate was checked on Linux/amd64 with Go 1.25.6, `GOTOOLCHAIN=local`, `GOWORK=off`, `CGO_ENABLED=1`, and the original module files (`-mod=readonly`). Work was serialized under the host experiment lock and limited to 0.6 CPU, 768 MiB RAM, `GOMEMLIMIT=512MiB`, `GOMAXPROCS=1`, and `-p 1`. No production container was rebuilt, replaced or restarted for H1.

| Check | Result |
| --- | --- |
| Eight server evidence unit tests with `-race` | Passed, package elapsed 1.047 s |
| Real QUIC authentication/transfer/evidence integration with `-race` | Passed, package elapsed 2.053 s |
| Application YAML parsing and logger wiring | Passed, package elapsed 0.010 s |
| Nine original ClientServer smoke/close tests plus ServerMasquerade, evidence disabled | Passed, package elapsed 19.144 s |
| Callback microbenchmark: one receive callback plus one ACK callback before authentication | 10.97 ns/op, 0 B/op, 0 allocs/op |
| Callback microbenchmark: the same pair after authentication | 203.1 ns/op, 0 B/op, 0 allocs/op |

Reproduction commands, run from `core` except the explicitly noted `app` test:

```sh
go test -mod=readonly -race -p 1 -count=1 -timeout 90s -run '^TestQUICEvidence' ./server ./internal/integration_tests
go test -mod=readonly -p 1 -count=1 -timeout 90s -run '^Test(ClientServer(BadAuth|UDPDisabled|TCPEcho|UDPEcho|HandshakeInfo|TCPClose|UDPIdleTimeout|ClientShutdown|ServerShutdown)|ServerMasquerade)$' ./internal/integration_tests
go test -mod=readonly -p 1 -run '^$' -bench '^BenchmarkQUICEvidenceCallbacks$' -benchtime 200ms -count 1 ./server
# From app:
go test -mod=readonly -p 1 -count=1 -timeout 90s -run '^TestServerQUICEvidenceConfig$' ./cmd
```

The short benchmark ran on a throttled virtual CPU and covers only the two callbacks. It excludes quic-go's frame construction, JSON serialization, sink I/O and complete transport behavior; it is not a throughput or CPU-overhead acceptance result. The real QUIC integration verified post-authentication ACK count, a positive RTT/window, observation freshness, rejected credentials, payload integrity and a matching close session.

At that initial revision, still required before production: a bounded tracing off/on QUIC workload comparison including throughput, CPU/allocations/GC and tail latency; actual deployed client interoperability and port-hopping checks; certificate-verification/security acceptance; injected network faults with known expected counter changes; and an observed rollout/rollback window. The fixture's `InsecureSkipVerify` does not weaken production defaults, but it also supplies no evidence that certificate validation passes. This branch remains disabled by default and is an observability candidate, with no demonstrated speed/stability benefit yet.


## Optional application copy evidence

`streamEvidence` is independently off by default and requires `enabled: true`. Its registry defaults to 256 observed copy streams per server and 32 per authenticated session, bounded to 1–1024 and 1–128 respectively. A full registry skips observation, not traffic, and increments that session's `application.untracked_streams_total`. Slots are removed on stream or connection close; a still-unwinding copy goroutine can retain its fixed state until its existing I/O returns, but no longer occupies a registry slot. Stream IDs are local sequence numbers associated with the random evidence session, not on-wire QUIC stream IDs.

This instrumentation covers only the existing `copyTwoWayEx` / 32 KiB `copyBufferLog` path used with a TrafficLogger, as in the deployment. The fast `io.Copy` path keeps its original dispatch and is explicitly counted as `application.fast_path_unobserved_streams_total`. Request parsing, hooks, initial putback data and outbound dialing precede the observed copy phase and are outside its byte counters. The original read → traffic accounting → Write sequence, short-write handling, first-error return and closing behavior remain unchanged. In particular, telemetry observes a short successful Write without adding a retry or changing its returned error.

Each direction has fixed-size atomic fields: read and actual Write-return byte totals, phase (`initial`, `read`, `accounting`, `write`, `finished`), phase age, pending bytes, maximum completed Write duration, short-write count, finished flag and fixed error category. `to_client` reads the outbound target then writes QUIC; `to_target` reads QUIC then writes the outbound target. `written_bytes` means accepted by that Write call, not acknowledged by the remote client. Read wait is ordinary absence of input and is not classified as a stalled write. Accounting wait is separate because the existing TrafficStatsServer uses a single instance-wide RWMutex in its per-chunk LogTraffic method and management API, so connections can contend on that lock; this is a possible coupling point, not a measured production bottleneck.

Each connection's `application` summary reports observed active copy streams, per-direction pending bytes, counts and maximum ages of accounting/write waits, and coverage losses. Empty wait sets have maximum age zero; interpret it together with the corresponding stream count. Initial unobserved direction phase age is -1. Atomics are sampled independently, so these fields are not a transactional snapshot.

`stream_sample` is emitted only for an in-progress accounting/Write with pending bytes and age at least one second. `stream_closed` carries a best-effort snapshot and explicitly reports both directions' `finished` flags; `best_effort` remains true until both directions finished. Its maximum completed Write duration is a lifetime maximum, not evidence of a new incident at close time.

A 1–2 second wait could start and finish between five-second samples. To retain that evidence, `stream_operation_slow_complete` is queued when an accounting/Write actually returns after at least one second. Its `operation` contains `stream_id`, `direction`, `phase`, `started_at`, `duration_us`, `requested_bytes`, `written_bytes` and `error_class`. Accounting's written count is zero because Write has not run yet. The start and capture timestamps are derived from the stream creation time plus monotonic elapsed time; duration does not use a potentially jumping wall clock. This rare completion path takes atomic snapshots and a nonblocking queue operation, without scanning the connection/stream registries or writing the logger inline. Normal chunks emit no events. A completion can arrive after a best-effort close while the second copy direction unwinds; associate by session/stream and operation timestamp rather than output ordering.

All application events share H1's single worker, bounded queue and rate budget. Slow logging or bursts can drop events; `dropped_events_total` and stream coverage counters remain necessary when interpreting a log window. Authentication failures cannot create application evidence, and no targets, addresses, credential/authentication identities, packet contents or arbitrary error strings are introduced.

### Application candidate acceptance, 2026-10-15 (HOLD)

The final application candidate (including slow completed operations) passed the
race-enabled focused core and original disabled-path regressions; verified local
TLS CA/name rejection, invalid-auth rejection, source-port rebinding, injected
1% packet loss, known pending transfer through a 3-second blackout, and the
configured 8-second idle close. This fault-test timeout is not a production default.

The actual JSON logging TCP performance matrix ran 14:09:09.589–14:10:34.100 UTC,
with one server child process isolated from the client/harness CPU measurement,
0.6 CPU and 768 MiB container limits, three interleaved pairs each for 1 and 8
independent QUIC connections. Each mixed run verified 130 MiB and 256 small
requests. The 1-connection pooled throughput ratio was 0.971056 and server CPU per
byte ratio 1.043208. At 8 connections throughput ratio was 0.929079 (7.09% loss),
violating the predeclared 5% gate, while CPU per byte ratio was 1.049269.
**This candidate is HOLD and must not be deployed.** Reverse-order pairs show
variance, but that does not waive the gate. Packet tracing constructs frame
metadata upstream even when callbacks ignore frames; this is an optimization
hypothesis, not yet a profiled attribution. The original run did not capture
actual cgroup throttling/peak memory or harness CPU; the next run must do so.
[Complete paired measurements](results/h1-20261015-full-packet-hold.json) include
single-pair small-only references, which are exploratory, not acceptance gates.

TCP copy evidence does not cover UDP relay or QUIC DATAGRAM queue overflow.
Traffic API accounting precedes successful writes and may count an oversized
UDP attempt again on fragmented retry; it is not delivered application bytes.

### Schema 2 candidate: packet opt-in and UDP application evidence

`packetEvidence` defaults to false even when aggregate evidence is enabled. The
four packet-header/frame callbacks remain nil in this mode: the pinned quic-go
source guards logging frame and ACK metadata construction on those callbacks.
The six packet counts/bytes/receive-send ages serialize as `null`, accompanied by
`packet_evidence:false`; they must never be interpreted as zero traffic. ACK,
loss, PTO updates, RTT/cwnd, MTU and close callbacks remain enabled. Full packet
evidence remains explicitly available, but is not part of the low-cost default.
The previous full-packet performance gate failure remains recorded above.

`udpEvidence:true` adds fixed per-authenticated-connection counters and eight
operation slots, independent of `packetEvidence`. Counters retain no address,
wire session ID, payload or error text. No additional UDP session registry exists.
Slots observe `receive_accounting`, `send_accounting`, `send_datagram`, `feed`,
`init`, and `target_write`; receive/read waiting is not classified as a stall.
`feed` encloses defragmentation, initialization and the actual target WriteTo;
its slot overlaps an init/write child slot, so slots must not be summed as
independent users or independent bytes. In-flight snapshots are best effort.
Slots are released when their operation returns, with fixed state retained by
existing blocked goroutines only; there is no new waiter goroutine. Saturation
increments `untracked_operations_total` while byte/error counters remain active.

The `udp` object counts received QUIC datagrams and parsed payload, actual target
read/write bytes and errors, send attempts, successfully enqueued datagrams and
payload bytes, too-large send errors, other send errors, serialization drops,
fragmentation attempts, and incomplete defragmentation returns. `defrag_pending`
is not a proven drop. `target_read_errors` can include ordinary close. Successful
`SendDatagram` means queued, not delivered, and initial oversized attempts are
excluded from enqueued bytes. Fragment counters describe retries, not unique
original messages. `receive_queue_drops_observable:false` explicitly records the
remaining pinned quic-go 128-frame user-space receive-queue overflow blind spot.
Kernel UDP drop counters cannot close this gap. No queue sizes are changed.

`udp_operation_slow_complete` reports only a phase that returns after at least
one second, via the existing bounded/rate-limited emitter. Its `udp_operation`
contains direction, phase, monotonic-derived UTC start, duration, requested bytes,
actual written bytes and an error class. Accounting/init written bytes are zero;
feed/target_write report WriteTo's actual result, and send_datagram reports queued
payload bytes on success. All return and close semantics, including silent
serialization drops and ignored temporary Feed errors, are preserved.

Authenticated records include `congestion_mode` (`bbr` or `brutal`) and
`negotiated_server_tx_bytes_per_second`; zero means BBR automatic congestion
control, not zero bandwidth. These are fixed once after the existing selection.

The next acceptance matrix records actual UTC bounds, separate server and
harness CPU, and container cgroup CPU counters, quota, current/peak memory and
limit before/after each run. cgroup memory peak is cumulative across the container,
not a per-run reset peak. Docker daemon logging CPU lies outside that cgroup.
TCP mixed runs retain the same 130 MiB/256-request denominator. An additional
UDP matrix verifies 4096 echoes of 1000-byte payload (8,192,000 total payload
bytes across both directions) through eight UDP sessions distributed over 1 or
8 QUIC connections, three alternating pairs each. These loopback/quota-limited
experiments bound instrumentation cost and do not predict Internet video bitrate.

### Packet opt-in/UDP candidate acceptance, 2026-10-15 (HOLD)

Core race and disabled original TCP/UDP regressions passed, as did the statistics
HTTP lock regression. Validated TLS trust/name/auth rejection, source-port
rebinding, injected loss and known pending TCP blackout/recovery again. Actual
UDP echo and 3000-byte fragmentation yielded 4000 exact read/write/enqueued
payload bytes, without counting the oversized first attempt twice. The first
8192-byte test exposed the unchanged protocol's 4096-byte serialization buffer
limit (client silent drop before the server); the acceptance payload was corrected
to 3000, not the protocol behavior. A 4.5-second injected blackout produced a
known pending SendDatagram wait and its slow completion on recovery. The initial
2.5-second observation was too short: PTO progress repeatedly released a few
queue entries, with no sampled phase age yet reaching one second.

[All 28 performance runs](results/h1-20261015-packet-opt-in-hold.json) preserve the
complete alternating pairs and small-only references, actual UTC bounds and
cgroup/process metrics. Packet metadata allocation overhead disappeared, but
TCP 1-connection pooled throughput ratio 0.947449, UDP 1-connection 0.941856,
and UDP 8-connection 0.947157 still fail the unchanged 0.95 gate. TCP
8-connection ratio was 0.979497. Server CPU per byte ratios were 1.069241,
1.042334, 1.043885 and 1.028916 respectively. **Still HOLD.** Healthy TCP closes
produced about 560 KiB of JSON per 130 MiB mixed run. The next candidate suppresses
healthy per-stream close records into fixed connection totals and removes the
duplicate UDP feed timer while retaining init and actual WriteTo timing.

### Routine-close aggregation candidate

Healthy/unremarkable short copy streams no longer emit a full close JSON each.
`application.completed_streams_total` counts tracked copy slots closed;
`incomplete_closes_total` records when either copy direction was still running.
`suppressed_routine_closes_total` counts suppressed records, not verified healthy
requests, and is not an evidence queue drop. Parser/hook/dial failures precede
copy tracing, and skipped/fast-path streams remain separately uncovered.
`terminal_directions` counts actual direction completion by none/eof/closed/
traffic_denied/timeout/other, including completion after slot removal. EOF and
QUIC stream cancellation code zero and Hysteria application close 0x100 are expected closure classes; this classification
does not alter the error returned by transport/copy functions. Snapshot totals
are independently sampled and the last connection close can precede an unwind.

A stream with a >=1s wait/completion, a short write or an abnormal error keeps its
`stream_closed` details. A late interesting direction completion emits
`stream_terminal_update` with the original stream ID, including when its initial
routine close was suppressed. These are snapshots, not separate failure counts.
All slow completed operations remain emitted. Ordinary live connections retain
periodic bounded summaries. No transport/copy behavior changes.

The duplicate ordinary UDP `feed` timer is removed: `init` still covers hook and
outbound setup, and `target_write` still covers actual WriteTo including fallback
DNS. Defragmentation remains a counter. UDP waits explicitly carry
`waits_best_effort:true`: independent atomics can straddle a slot reuse and must
not be interpreted as a transactional operation snapshot. Slow completion records
are assembled by the operation's owning goroutine before releasing its slot.

The final comparison uses four alternating pairs (balanced off-first/on-first)
for both TCP and UDP. TCP still verifies 130 MiB and 256 small requests per run;
UDP verifies 16384 1000-byte echoes (32,768,000 bytes, both directions), increasing
run duration to reduce quota-boundary noise. Prior failing matrices remain above;
the 5% throughput and 20% server CPU/byte gates remain unchanged.
