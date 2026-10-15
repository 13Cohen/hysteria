# Next BBR evidence candidate: design only

The pacer candidate remains on hold after H2, H3 and H4. This note records a
read-only source analysis and a possible bounded observer. No observer code,
new performance experiment or production BBR change is included in this plan.

## What the pinned code establishes

`core/server/server.go` selects BBR only after authentication succeeds.
`core/internal/congestion/utils.go` creates a fresh sender, and the pinned
quic-go `sentPacketHandler.SetCongestionControl` injects the existing RTT provider
before publishing that sender. Thus the transport may already have handshake
RTT measurements even though the new BBR bandwidth estimator has no samples.

`core/internal/congestion/bbr/bbr_sender.go` starts with 32 times the initial
packet size. The pinned UDP initial size is 1,280 bytes, so its initial window
is 40,960 bytes. The old pacer computes `bandwidthEstimate * cwndGain`, which
falls to the unchanged 65,536 bytes/second minimum before an estimate exists.
The candidate instead calls `PacingRate()`: while its cached rate is zero, that
function returns `2.885 * initialWindow / getMinRtt()`. With 160 ms RTT the
computed rate is 738,560 bytes/second (approximately 5.91 Mbps per connection).
Only if neither BBR nor the RTT provider has a minimum RTT does the 100 ms
fallback apply, giving 1,181,696 bytes/second. These are code-derived budgets,
not observed production send rates or promised throughput.

`core/internal/congestion/common/pacer.go` uses a minimum initial burst budget
of ten packets and a rate-dependent alternative of four minimum pacing delays.
At the rates above, both variants therefore start with the same 12,800-byte
burst budget. The candidate can replenish that budget faster afterward; it is
incorrect to assert that its first burst necessarily contains more packets.
For example, ignoring other limits, a 1,280-byte budget refill takes about
19.53 ms at the old floor versus 1.73 ms at the candidate's 160 ms fallback.
The one-millisecond minimum pacing delay, cwnd and ACK progress still constrain
actual sending.

When the first usable bandwidth sample arrives, `calculatePacingRate` can first
initialize the cached rate to `initialWindow / providerMinRTT`, without the high
gain. Subsequent startup updates use a nondecreasing target based on the 2.885
pacing gain. The constructor separately sets the congestion-window gain to 2.0,
which is what the old pacer continues to use. On reaching full bandwidth,
DRAIN uses approximately 0.347 times estimated bandwidth in the candidate,
while the old pacer remains approximately twice that estimate. PROBE_BW then
cycles candidate gains 1.25, 0.75 and 1, while the old pacer still uses the cwnd
gain. The minimum pacing floor can clip either value at sufficiently low rates.

Four simultaneously ready senders could therefore refill more aggressively
at first and then honor a much slower drain phase. This is a plausible source
of a short-transfer tradeoff, not a demonstrated causal explanation. H2/H3/H4
created/authenticated their connections sequentially and warmed them before
starting application transfers together; they did not measure simultaneous
cold authentication. H4 did reproduce the short four-connection regression
under H3's warmup, so longer transfers do not waive that regression.

## Why existing evidence cannot prove the phase

H1 records RTT, cwnd, inflight and ACK/loss/PTO progress. It has no internal BBR
phase, recovery state, pacing gain, effective pacing budget rate or bandwidth
estimate. Its test interval is one second, longer than several 160 ms RTTs and
short transitions. The internal BBR state-tracer calls are commented out.
A 5,120-byte cwnd observation cannot distinguish PROBE_RTT from recovery or
another limiting state. The H3/H4 anonymous timelines remain useful supporting
evidence, but are insufficient for phase attribution.

## Minimum proposed implementation

The source scope can remain entirely in `13Cohen/hysteria`: its internal BBR
package, the local congestion-selection helper and H1 server adapter. Existing
quic-go custom-congestion interfaces are sufficient; no new quic-go fork is
needed for this evidence. PCS would only receive collector/parser support.

Add an optional observer that is nil by default. It receives immutable values
copied on the existing congestion-processing path; the asynchronous H1 worker
must never read BBR's mutable internals directly. The observer should cover:

- Initial model state once the RTT provider is installed, with an explicit RTT
  source (`bbr_min`, `provider_min`, or `fallback_100ms`) and estimate validity.
- First send once, including initial window, actual effective pacer rate and
  pre-send pacing budget. This distinguishes the equal first burst from faster
  refill without adding packet-by-packet logging.
- Major phase and recovery transitions, with monotonic-relative capture times,
  old/new enum and duration. Count entries and accumulated dwell time in fixed
  arrays, so dropping a transition log does not silently erase the denominator.
- A bounded latest state, updated at most ten times per second using the already
  available congestion-event timestamp. Fields: phase, recovery, cycle index,
  pacing gain, bandwidth estimate, model pacing rate, **actual pacer rate**,
  minimum RTT and its source, effective cwnd, inflight, application-limited flag
  and round number. Model and actual rates must be distinct: the old pacer can
  ignore the computed model rate. Units must be explicit bytes/second.

A gain-cycle change need not create a separate log record on every RTT. Keep
fixed counters/latest values and let the existing H1 periodic sample carry
those values. Major transition and first-send events may use the existing
bounded global queue/rate limiter, with an additional small per-connection
burst limit and explicit suppressed/dropped counters. Do not block the sender
on logging, serialize JSON in congestion callbacks, allocate per packet, keep
unbounded transition histories or emit raw packet/header/frame metadata.

The existing authenticated session state can own a fixed atomic snapshot with
a generation guard. Readers retry a bounded number of times or mark the result
unknown/inconsistent; fields from different updates must not be combined as one
coherent observation. The writer-path serialization assumption must be checked
and exercised under race tests. Known zero bandwidth estimates differ from
unknown observations, and snapshots need an age and validity marker.

Binding occurs only on a successful-authentication path, with the initial
snapshot published through H1's authentication gate. Rejected authentication
must remain silent. Preserve the current ordering and behavior of TLS,
authentication, congestion selection and response generation. Close removes
tracking and stops publication; a final partial snapshot is labeled best effort.
Existing connection/event caps and privacy restrictions remain in force.

## Validation before any release

First verify no-estimate/provider-RTT/fallback-RTT cases, the first-send budget,
STARTUP/DRAIN/PROBE_BW/PROBE_RTT and recovery transitions, rate units, auth gating,
bounded overflow, generation consistency, concurrent close and a blocked logger.
Run actual observer-off/on network comparisons to account for all callback and
logging cost; atomic microbenchmarks alone are insufficient.

Then compare old and candidate pacing with the **same observer enabled in both**,
retaining the existing H4 warm workload and adding a separately planned cold
four-connection start only if authorized. Freeze those parameters and keep all
prior data. Do not change congestion gains, queue sizes, windows or inflight
semantics while collecting this evidence. The original throughput/reliability
and CPU gates remain; these instrumentation steps do not authorize a BBR rollout.
