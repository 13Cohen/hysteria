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
```

The interval is limited to 1–60 seconds; the registry to 1–512 connection slots. Defaults are 5 seconds and 128 slots. Slots include unauthenticated QUIC handshakes so a handshake flood cannot create an unbounded evidence registry. If no slot is available the connection proceeds without evidence; its transport is not rejected or altered. `untracked_connections_total` reports that coverage loss in subsequent authenticated-session records; if no tracked session authenticates, H1 emits no standalone server health record. The original TLS/authentication, masquerade, QUIC windows, idle timeout, BBR selection, and `DisablePathManager=true` port-hopping behavior remain unchanged.

## What is recorded

Only successfully authenticated sessions emit `quic_evidence` records. They have a fresh random session namespace per server instance and an internal tracing sequence; they do not expose on-wire QUIC connection IDs. `stats_connection` links to the existing traffic API's randomly assigned connection number, but that 32-bit number alone is not a globally unique identifier. Repeated authentication on the same QUIC connection does not allocate a new session or emit a second authentication event.

Event types are `authenticated`, periodic `sample`, and `closed`. Each record contains schema version, capture time, session age, packet/byte totals, acknowledged packet count, declared loss counts by reason, current/maximum PTO count and positive PTO-count callback updates, RTT gauges, congestion window, bytes/packets in flight, MTU, and ages of the most recent observed receive/send/ACK/metrics event. Close classification includes only a fixed category, numerical protocol code, and local/remote flag where meaningful. Raw error messages are excluded.

`pto_positive_updates` counts calls to `UpdatedPTOCount(n)` where `n > 0`; it is deliberately not named total timer firings. In the pinned dependency's `internal/ackhandler/sent_packet_handler.go`, the positive callback follows the normal PTO timer branch; other call sites reset it to zero. The unvalidated-address special branch increments PTO without the same callback, so the callback is not a universal count of every internal timer path. Authentication gating further excludes pre-authentication activity.

The callback semantics above were checked against the pinned dependency: [PTO update call sites](https://github.com/apernet/quic-go/blob/eb32f8aec5e2/internal/ackhandler/sent_packet_handler.go), [connection context and tracer association](https://github.com/apernet/quic-go/blob/eb32f8aec5e2/server.go), and [packet callback frame construction](https://github.com/apernet/quic-go/blob/eb32f8aec5e2/connection.go).

No client/server addresses, domains, request targets, authentication IDs, passwords, payloads, packet numbers, token contents, or frame contents are exported. Existing upstream logs are unaffected and retain their prior behavior. Authentication failures and unauthenticated closes produce no new session records.

Counters start after authentication and describe callback observation time; a later ACK can refer to a packet sent before authentication. Packet byte counts include protocol overhead and are not application delivery counters. A packet declared lost can later be acknowledged; declared-loss and ACK counts must not be interpreted as mutually exclusive outcomes or direct path loss measurements. Gauge fields are independently sampled atomics, not a consistent lock-held snapshot of the transport. `-1` age means no callback has yet supplied that observation; zero-valued RTT/window/MTU fields may likewise be unobserved. The gauge-age field must accompany stale measurements.

An idle timeout is a transport close reason, not by itself a confirmed user failure. Pending application work, useful-byte progress and blocked writes require application-level evidence and controlled end-to-end probes. This candidate does not yet log per-stream requests/outcomes, exact write-blocking locations, DNS attempts, or a complete request denominator. Do not infer users from shared password identities or infer device reconnection from a new session alone.

## Bounds and overhead

Packet callbacks only update fixed-size atomics; they do not take the registry mutex, write logs, call a sink, retain frame slices or RTT pointers, or create worker goroutines. Connection setup/authentication/close use the short-lived registry mutex. Periodic snapshots run in one background worker per server instance. A 64-entry lifecycle-event queue drops records rather than blocking. A token bucket per server instance replenishes 64 records/second, with a bounded burst of `maxConnections + 64`, so a normal periodic snapshot batch fits while sustained floods remain limited. `dropped_events_total` is cumulative and includes both bounds. It must be checked before using logs as a complete population.

Logger backpressure can block that one worker, but not packet callbacks or proxy shutdown. At most one emitter goroutine **per server instance** can remain blocked until the destination resumes. Repeated `NewServer`/`Close` against a permanently blocked sink can retain one worker per old instance because the logger interface has no cancellation primitive; the bound is not a process-wide no-leak guarantee. The CLI normally creates one server instance and exits on a fatal serving error. The repeated-instance test verifies those workers exit when the sink resumes. Shutdown is best effort and can discard queued final records. A snapshot already taken while a connection is closing can race with its final event; order by capture time and event type, not output position alone. Evidence may be incomplete during slow logging, saturation or shutdown.

There is still transport overhead: enabling the upstream receive/send packet callbacks causes quic-go to assemble frame metadata, even though H1 does not persist it. Callback-only allocation/latency benchmarks cannot establish complete end-to-end cost. Before production, compare tracing off/on under the same bounded authenticated flow mix, inspect CPU/allocations/GC and p95/p99 latency, validate no sensitive fields escape, and confirm counters against known injected ACK/loss/idle sequences. No throughput benefit is claimed by adding observability.

## Why no quic-go fork yet

The pinned dependency already provides `Config.Tracer` and `logging.ConnectionTracer` callbacks for the measurements used here. Hysteria owns its BBR implementation and can extend that state reporting separately. Precise internal path-switch decisions, send-queue scheduling or flow-control blocking points would need a later API extension or dependency fork if existing evidence proves them necessary. Merely switching `DisablePathManager` off would alter the deployed port-hopping path and is outside H1.

## Candidate validation

Targeted tests cover authentication gating, missing-observation sentinels, counter aggregation/PTO resets, typed close classifications without error-string leakage, slot reuse/caps, bounded queue/rate, blocked logger isolation including repeated instances, concurrent snapshots, and application configuration parsing. A real local QUIC client test rejects bad authentication, echoes payloads, checks authenticated ACK/RTT/window records and the close event. It follows upstream fixture tests in skipping certificate verification and is not a certificate-policy acceptance test. Run race tests and callback benchmarks under the host's shared resource lock. Existing core integration tests are required before any rollout to check authentication, transfer and timeout behavior with diagnostics disabled. A controlled live tracing off/on overhead comparison remains a separate acceptance gate.

Only push the `pcs/quic-evidence` branch. Upstream release workflows react to `app/v*` tags and master-specific push triggers; do not push release tags or update master while validating this candidate. PCS image/source pins remain unchanged until a separately reviewed rollout.


## Validation recorded on 2026-10-15

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

Still required before production: a bounded tracing off/on QUIC workload comparison including throughput, CPU/allocations/GC and tail latency; actual deployed client interoperability and port-hopping checks; certificate-verification/security acceptance; injected network faults with known expected counter changes; and an observed rollout/rollback window. The fixture's `InsecureSkipVerify` does not weaken production defaults, but it also supplies no evidence that certificate validation passes. This branch remains disabled by default and is an observability candidate, with no demonstrated speed/stability benefit yet.
