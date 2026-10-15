# Bounded UDP DNS result reuse experiment

Status: opt-in direct-outbound source candidate; production is unchanged.
The source branch `pcs/udp-flow-resolve` is based on 7883462, with the production
v2.6.4 source at 2146852. Only `extras/outbounds` and this experiment record are
part of this change. App configuration mapping is a separate coordinated change.

## Evidence and scope

`udpConnAdapter.WriteTo` constructs a new `AddrEx` for each datagram. For a domain
with no supplied `ResolveInfo`, `directOutboundUDPConn.WriteTo` therefore calls
`net.LookupIP` again for each packet. The core UDP receive/feed path is serial
within a QUIC connection, so those waits can also delay later UDP datagrams.
The live investigation measured 244 target datagrams and 488 DNS queries in one
10-second interval, consistent with one A and one AAAA query per datagram.
Measured DNS response times were usually small (p50 1.24 ms / p95 2.11 ms,
maximum 50.56 ms); this work does not establish DNS as the sole cause of the
observed sustained low video throughput.

This candidate only changes the **default direct outbound's system-DNS fallback**.
It does not repair the existing adapter behavior that drops a custom resolver's
initial ResolveInfo or ACL target rewrites, re-evaluate ACL routing for later
full-cone destinations, or change SOCKS remote DNS. Those are separate findings
and require policy/compatibility design. Existing caller-provided ResolveInfo
always wins. IP literals, including IPv6 zone syntax, take the original path.

## Behavior and bounds

`DirectOutboundOptions.UDPResolveReuse` is **false by default**. A true value
creates a cache owned by that individual UDP socket:

- At most **32 complete, exact host+port keys**; different hosts and ports do
  not borrow each other's results. Entries never cross UDP sockets/users.
- Only successful results with at least one address and no resolver error are
  retained. Empty, negative and partial-error results are not cached. Existing
  outbound handling of partial-error results is preserved.
- A result expires **10 seconds after its lookup completes**. This is a local
  reuse lifetime, **not DNS TTL support**: net.LookupIP does not expose record
  TTLs. Hits do not extend the deadline; expiration forces resolution without
  serving stale results on failure. DNS changes can remain unseen for up to
  this local lifetime, including authoritative TTLs shorter than 10 seconds.
- Expired entries are removed on access/insertion; the oldest expiry is evicted
  at the cap. There are no timers, new worker goroutines or global cache.
- The mutex protects only bounded map operations, never LookupIP or WriteToUDP.
  Concurrent misses may still perform duplicate lookups, rather than waiting
  behind a new global or per-socket lookup lock.
- IP slices are copied on storage and return. Cached addresses are not exposed
  to caller mutation. A local send failure evicts only the exact entry used by
  that write; a late old failure cannot delete a newer replacement.
- Close clears retained entries and prevents in-flight lookups from refilling
  the cache. It does not wait for DNS. Existing system lookups are not newly
  cancellable and may finish after close before the caller receives an error.

The first packet, expiration, cache misses, and lookup failures still incur
synchronous DNS latency in the core feed path. A successful UDP send also does
not prove remote delivery, so silent destination failure is only bounded by
expiry; it is not an active reachability/failover mechanism.

## Acceptance

The fixture uses a local Go DNS resolver directed exclusively to an in-process
loopback DNS server plus a loopback UDP receiver. It sends 16 distinct patterned
payloads through the real adapter/direct path, checks every byte and counts A
and AAAA requests. Off/on runs use the same binary. The DNS fixture inserts
5 ms delay per query to make avoidable serial work visible; its timing is not
a production speed estimate. Other tests cover expiration without sliding
renewal, exact host+port identity, the 32-entry bound, failure and recovery,
partial errors, deep-copy ownership, concurrent access, nonblocking cache hits
and close during a blocked lookup, and old-send-failure/new-entry ordering.

The source candidate `fb236a9` passed these checks on 2026-10-15, using Go
1.25.6, 0.25 CPU / 512 MiB RAM / 1024 MiB RAM+swap, GOMAXPROCS=1 and serial
compilation under the shared experiment lock:

| Run | Result | Relevant evidence |
| --- | --- | --- |
| Normal outbounds, ACL and speedtest packages | PASS | 16/16 complete UDP payloads per mode; DNS 32 queries off versus 2 on |
| Same three packages, race detection, 3 repetitions | PASS | The same query counts and intact payloads in all repetitions |
| Final focused outbounds race run with isolated IPv6 enabled | PASS, 1.333 s | auto/4/46/6/64, IPv4/IPv6 bind, supplied resolution, literal bypass and local send failure/recovery all passed; no skips |

The initial containers had IPv6 disabled and explicitly skipped IPv6 receiver
cases. The final run enabled IPv6 only in its isolated network namespace and
closed that gap. IPv4/IPv6 bind and send-recovery tests were added before this
final run. An initial overly broad package selection was stopped and narrowed
to exclude unrelated Internet-dependent DoH tests; it is not a successful run.

Across all five recorded off/on pairs, 16 payloads caused **32 versus 2 DNS
queries**, a 93.75% reduction in this fixture. In the normal run, total loopback
send/read time was 98.532 ms off versus 6.094 ms on; final race-run times were
186.095 ms versus 9.261 ms. These include an artificial 5 ms DNS response delay
and CPU quotas, so they are neither Internet speed results nor a prediction of
production improvement. All payloads were checked byte for byte.

Commands, resource limits, exact observations, selected outputs and raw-output
hashes are recorded in
[`results/20261015-udp-resolve-reuse/results.json`](results/20261015-udp-resolve-reuse/results.json).
No production deployment or throughput load is included.

## Integration and rollback

The app agent will map an explicit direct outbound's YAML `udpResolveReuse`
boolean to `DirectOutboundOptions.UDPResolveReuse`. The existing implicit
default outbound remains disabled. A production canary must use an explicit
single direct outbound, then compare DNS queries per UDP send, UDP feed/write
stage waits, payload errors, observed rate and CPU under comparable real usage.
The option can be turned off using the same binary; an image rollback remains
available. DNS query reduction alone is not acceptance of an overall streaming
speed or stability improvement. TLS/authentication, QUIC versions and target
selection policy are not changed by this cache.
