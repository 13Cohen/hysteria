# UDP datagram sizing, queue evidence and reassembly follow-up

Status: read-only source analysis and isolated experiment design. No production,
H1 release, transport dependency, negotiated parameter, queue size or protocol
behavior is changed. The new pure-memory diagnostic has been prepared; its
execution status is recorded below. The source basis is Hysteria v2.6.4
2146852 and its pinned apernet/quic-go eb32f8aec5e2. Later H1 counters are useful
evidence but do not make the underlying transmission reliable.

## 1. A negotiated frame cap can dominate path MTU

Hysteria `core/internal/protocol/proxy.go:23` fixes
`MaxDatagramFrameSize = 1200`. The server (`server.go:58` in 2146852) and official
client (`client.go:88`) advertise that value as their QUIC transport parameter.
A local receive advertisement constrains the peer's sending direction; raising
only the server's advertisement does not raise the client's advertised receive
limit for server-to-client traffic. Shadowrocket's actual offer still needs to
be observed rather than inferred from the official Go client.

Pinned quic-go `connection.go:2622` computes the accepted SendDatagram payload as:

```
min(DatagramFrame.MaxDataLen(peer.MaxDatagramFrameSize), currentMTUEstimate)
```

`internal/wire/datagram_frame.go:77` accounts for one frame-type byte and a
length varint. At a 1200-byte frame limit, the data can therefore occupy **1197
bytes**. These bytes must include the Hysteria UDP-message header, not just the
proxied UDP payload. Hysteria's header is:

```
8 bytes fixed fields + varint(address byte length) + address bytes
```

The initial packet-size default is 1280 QUIC bytes. Initial SendDatagram's MTU
estimate is `1280 - 1 - 20 - 16 = 1243`, so a peer's 1197-byte data cap is already
tighter. PMTU discovery is bounded by the local maximum packet buffer (1452)
and the peer's max_udp_payload_size. The probe algorithm waits five smoothed
RTTs between probes, can tolerate up to two lost probes before its search bound
changes, and stops within roughly 20 bytes. IPv4/IPv6 and tunneled paths can
have different actual limits. None of that alone removes a 1200-byte peer
DATAGRAM-frame advertisement.

### Why the observed sizes fit fragmentation

The privacy-preserving header observation counted **UDP length including its
8-byte UDP header**, not application bytes. Its 15-second window saw 350 target
UDP lengths of 1358 and 350/347 outgoing lengths of 1235/247. A later 10-second
window saw 232 large target packets and 229 of each outgoing size. Those are
aggregate correlations, not paired packets or a network-loss estimate; the
capture was IPv4-only at a veth and can be affected by offload.

One exact size construction consistent with those observations is:

| Component | First fragment | Second fragment |
| --- | ---: | ---: |
| Inner UDP payload (1358 - 8 = 1350 total) | 1169 | 181 |
| Hysteria header, for a 19-byte address | 28 | 28 |
| QUIC DATAGRAM header | 3 | 3 |
| Short header + AEAD tag, e.g. CID length 8 / packet-number length 2 | 27 | 27 |
| UDP header | 8 | 8 |
| Observed UDP length | **1235** | **247** |

Connection-ID length and packet-number encoding are illustrative here; the
quic-go default CID length is 4 and an actual peer can choose another length.
Piggybacked ACK/control frames also change packet lengths. This exact arithmetic
supports the fragmentation hypothesis but does not prove the live peer's
transport parameter, address length or the identity of any encrypted packet.
Capture only numeric negotiation/header-length/fragment counters to verify it;
no addresses, payloads or raw connection IDs are needed.
In that construction the two UDP lengths total 1482 rather than the original
1358, an extra 124 bytes at that layer (about 9.1%, before IP headers). Two
packets do not by themselves imply halved bandwidth; packet processing,
congestion and successful reassembly still need separate measurements.

### A boundary to test before ever raising the cap

`connection.go:1786` initializes/updates currentMTUEstimate differently: after an
ACK it stores the raw `mtuDiscoverer.CurrentSize()` rather than applying the
initial estimateMaxPayloadSize subtraction. SendDatagram later uses that value
as DATA capacity. Meanwhile `packet_packer.go:657–667` silently pops an oversized
queued DATAGRAM if it cannot fit a packet with no ACK. These source paths suggest
an accepted-but-not-packed boundary when peer frame limits are raised. Path
migration resets the MTU finder (`connection.go:1118` / `:797`) without an obvious
matching reduction of the separate estimate in those functions.

This is a **test hypothesis**, not a reproduced defect or an explanation of the
live incident. The existing 1200-byte peer cap may mask the boundary. Preserve
that cap and path discovery until a controlled test proves SendDatagram's
accepted sizes fit after handshake ACKs, probe updates and path changes.

## 2. Queue semantics and the missing evidence

Pinned `datagram_queue.go` has **32 send entries and 128 receive entries per QUIC
connection**, shared by that connection's UDP sessions. These counts represent
DATAGRAM frames, including application fragments, not original UDP messages.

- Add blocks when the send queue is full, releases its mutex while waiting and
  wakes on dequeue or connection close. It does not add an independent per-call
  cancellation deadline. A server-side blocked SendDatagram delays that target
  socket's receive loop; other UDP sessions share the same queue/cwnd/pacer.
- A full receive queue drops the newly received DATAGRAM. Only a debug log is
  emitted; successful outer QUIC acknowledgment does not prove application
  dequeue or inner UDP delivery.
- Core server Run/Feed is synchronous across UDP sessions in one connection.
  DNS or target writes can stop application draining and contribute to receive
  queue pressure. Separate QUIC connections have separate datagram queues;
  they still compete for process CPU, socket/kernel resources and link capacity.
- On the server, receive-queue drops concern **client-to-target traffic** and
  send-queue waits concern **target-to-client traffic**. Do not attribute the
  official Go receive-queue implementation to an unknown Shadowrocket client.

`logging.ConnectionTracer` has no datagram queue-depth or queue-drop callback.
`DroppedPacket` concerns packet-level rejection and is not called for the full
DATAGRAM receive queue. Packet frame callbacks expose only a DatagramFrame
length and could give a coarse receive/dequeue difference, but pending work,
closes and tracing overhead make that neither an exact drop counter nor the
preferred always-on approach. Global QUIC debug logs are inappropriate because
they are expensive and can include connection metadata.

An optional **future** quic-go snapshot API can expose bounded per-connection
counters without emitting per-packet logs:

- send/receive queue current entries and bytes, high-water marks;
- enqueue/dequeue counts and bytes;
- receive-queue-full drops;
- send queue-full wait count, sum/max duration, fixed duration buckets;
- packer oversized-DATAGRAM drops, separate from successful packet packing;
- pending entries discarded at close, to make interval accounting explicit.

Update normal counters under existing queue mutexes. Only take timestamps when
Add actually waits; do not add a clock call for every successful packet. Distinct
packer-pop reasons are needed because Pop currently means either pack or drop.
A sampler may snapshot both queues in short separate critical sections; mark it
non-atomic across directions. Keep collection off by default, retain 32/128 caps,
avoid new goroutines/registries per packet, and export only through bounded,
authenticated connection records. Never attach payloads, domains, client
addresses, credentials or unbounded session labels. This requires an independent
quic-go source change/fork later; none is made in this round.

Before that, existing one-time Sent/ReceivedTransportParameters callbacks can
record just numeric max_datagram_frame_size and max_udp_payload_size, alongside
H1's observed too-large limit and current/initial MTU. Negotiation occurs before
application authentication: retain bounded numeric fields then publish only
once the connection authenticates. An unset MTU callback value must not be
interpreted as disabled discovery or a measured MTU of zero.

## 3. Reassembly and accounting findings

`core/internal/frag/frag.go:37–55` keeps only one PacketID. Arrival of a different
packet replaces the prior partial assembly. Intra-packet reordering can work,
but A0,B0,A1,B1 interleaving can discard both complete packets even if all four
fragments arrived. Official client code uses that defragger per UDP session;
Shadowrocket's implementation is unknown. Two independent session defraggers
do not share this state. Packet loss/reordering can therefore affect fragmented
traffic differently from unfragmented traffic, without violating QUIC DATAGRAM's
unreliable delivery semantics.

The pure-memory `core/internal/frag/interleaving_diagnostic_test.go` checks exact
payload contents in ordered, within-packet reordered, duplicate, interleaved and
missing-fragment sequences. It does not change the defragger or treat loss as a
desirable permanent contract. `PCS_REQUIRE_INTERLEAVED_DEFRAG=1` enables a future
bounded-reassembly acceptance gate. No protocol fix is proposed for deployment
until memory/frame-count bounds, expiry, PacketID reuse, adversarial fragment
input and cross-session isolation have their own tests.

Execution status: **prepared, not yet run** (shared experiment lock unavailable
at the first attempt). No runtime conclusion is claimed yet.

Separately, baseline server.go:358 accounts len(msg.Data) **before** SendDatagram.
If a 1350-byte message returns too-large, sendMessageAutoFrag retries its two
fragments and calls the accounting hook again. The source path can therefore
account 1350 + 1169 + 181 = 2700 while only 1350 unique payload bytes are intended.
H1 target-read versus enqueued-unique-byte evidence can distinguish this from
actual throughput. A rejecting traffic-limit hook could also make the retry
behavior consequential. Keep any accounting correction separate and preserve
rate/limit semantics; no such correction is included here.

## 4. Staged experiments and acceptance gates

| Experiment | Controlled variables | Required observations and pass criteria |
| --- | --- | --- |
| Negotiation and MTU boundary | Peer frame limits 1200/1400/16383; packet sizes 1280/1452; before/after first 1-RTT ACK, PMTU update and path reset | Accepted payloads fit or return an explicit too-large error; no unreported packer drops; capture only numeric caps and frame sizes |
| Fragment-size boundary | Synthetic address lengths 19 and IPv6-sized; payloads 1100/1169/1170/1200/1350/1440 | Exact payload reconstruction and fragment counts; reproduce the 1235/247 construction under explicit CID/PN/header conditions |
| Send/receive queue unit tests | 31/32/33 sends with gated dequeue; 128 plus 10 incoming frames with paused receive | Full-send wait and close unblock counted exactly; exactly 10 receive drops and 128 retained frames; instrumentation changes no delivery/order behavior |
| Serial Feed versus independent connection | One slow target/DNS and one healthy UDP session in same QUIC connection; separate-connection healthy control; pauses 5/50/500 ms | Queue depth/drop and Feed/target-write waits distinguish application drain delay from network pacing; cold/expired misses remain visible after DNS reuse |
| Reassembly | Ordered, intra-packet reordered, duplicate, interleaved and missing fragments | Full byte verification; distinguish packet loss, reordering and implementation assembly eviction; any future fix must be finite in bytes, packets, age and sessions |
| Accounting | One whole 1350-byte too-large attempt followed by two fragments, plus rejection and send-error cases | Report unique target bytes, attempted/enqueued bytes and billing callbacks separately; decide limiter semantics before modifying them |
| End-to-end canary | Current client; one versus several UDP sessions and independent clients; fixed offer rate and route; current queue sizes | Compare unique payload goodput, latency, drops, retransmission/ACK/loss/PTO, CPU/RSS and per-connection fairness. DNS query reduction or enqueue count alone is insufficient |

Start with pure-memory boundaries and existing H1 live counters. Use the shared
resource lock for any later build/network experiment, keeping candidate toggles
and comparable baseline/off runs. Do not enlarge queues to make waits disappear:
that can move pressure into latency, memory and later loss. If the peer's receive
cap is the cause of fragmentation, any larger-datagram experiment must use a
compatible client and respect the negotiated cap; a server cannot safely ignore
it. Authentication, encryption, congestion control, anti-amplification and
multi-connection isolation remain acceptance requirements.
