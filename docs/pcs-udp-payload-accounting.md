# UDP payload accounting candidate

Base: `9baeab7` in `13Cohen/hysteria`, derived from upstream v2.6.4.
This candidate is separate from the current release and has not been deployed.

## Problem and contract

The server calls `udpIOImpl.SendMessage` for the original target UDP payload.
That method calls `TrafficLogger.LogTraffic(id, 0, len(payload))` before
serialization and `SendDatagram`. If QUIC reports `DatagramTooLargeError`,
`sendMessageAutoFrag` splits the same payload and calls `SendMessage` again for
each fragment. Successful fragmentation therefore records the original payload
twice: once for the rejected whole datagram and once across its fragments.

Account and authorize each original target payload exactly once, before any
serialization or send. Only the private retries of this same authorized payload
skip repeated accounting. There is no connection-wide or mutable skip flag.
Each independently submitted original message still invokes the logger,
including zero-length payloads and messages submitted concurrently.

`LogTraffic=false` still closes QUIC with the traffic-limit error and returns
`errDisconnect` before any `SendDatagram`; even a too-small serialization buffer
does not bypass this check. Authorization covers the full original payload, not
each fragment independently. A logger with external, changing authorization
state is checked again on the next original payload; this change deliberately
removes the redundant intermediate callbacks for fragments of an already
authorized payload.

The existing attempted-payload semantics are retained: serialization drops,
ordinary send errors, and partial fragment failures do not refund the original
accounting charge. The API therefore measures accounted original payload, not
client delivery. A fragment failure still immediately ends the send and returns
that error. A second too-large result from a fragment is still returned without
recursive fragmentation.

## Implementation and evidence

`SendMessage` delegates to a helper with accounting enabled. Its private
`sendFragment` entry point delegates with accounting disabled and is selected
only after the first `SendMessage` returns `DatagramTooLargeError`.
Other internal `udpIO` implementations retain their previous `SendMessage`
behavior. `udpIOImpl.Conn` uses a package-private three-method interface so
tests can exercise the real serialization and fragmentation path without
creating network sockets.

H1 attempt/outcome evidence remains per actual send attempt: original too-large
attempts, fragment attempts, successful enqueue counts and payload bytes,
serialization drops, and errors retain their previous definitions. Accounting
wait evidence now observes one logger invocation per original payload.

For an original payload split into N fragments, logger calls fall from `N+1`
to `1`. This removes duplicate statistics and redundant logger calls. It does
not by itself establish a latency or throughput gain, and it is not proof of
the cause of video stalls. QUIC framing, congestion control, padding, stream
behavior, and fragment sizing are unchanged.

## Verification

Existing UDP TrafficLogger integration coverage checks small payload success
and rejection, but not too-large fallback, partial fragment failures,
zero-length accounting, or serialization drops. Existing H1 UDP tests cover
serialization evidence but did not attach a logger to that test.

The new pure-memory tests exercise the actual production accounting and
fragmentation helpers with a fake QUIC boundary:

- Small and zero-length payloads each invoke the logger once. The existing
  sender serializes zero data, while the receiver rejects empty payloads; this
  patch preserves both rules.
- A 250-byte payload split into 100/100/50 bytes accounts 250 bytes once,
  while retaining four send attempts and three successful enqueues.
- Initial ordinary errors, first/second fragment errors, and a fragment's
  second too-large error retain their error and attempt semantics.
- Serialization drops still account once and silently return nil.
- Denied payloads, including zero bytes, close before serialization or send.
- Sixteen concurrent original messages each receive independent accounting;
  a nil logger retains normal fragmentation.

Candidate verification passed on 2026-10-15 from 15:29:57 to 15:30:11 UTC
with Go 1.25.6. The selected test binary ran in 0.016 seconds; the wall window
also includes package compilation. All eight table cases, denial checks, and
concurrent/nil-logger checks passed. The first run at 15:27:40–15:29:17 UTC
exposed a test-double assumption: it tried to parse a zero-data datagram with a
receiver that intentionally requires data. The test was corrected to inspect
its serialized header; production code was unchanged between those runs.
The initial run did not include race instrumentation; the follow-up below
adds that check. End-to-end integration and performance acceptance remain
outside this candidate's verification.

Tests use the shared `/run/lock/pcs-proxy-experiment.lock`, 0.25 CPU, 512 MiB
memory, one Go worker, and a network-disabled container. UTC start/end and
exact results are recorded under `docs/results/udp-accounting/`. These are
correctness tests, not a throughput experiment or production acceptance.

A genuine regression baseline was checked on 2026-10-15 from 15:33:24 to
15:33:38 UTC. An independent archive of candidate `5140d461e1b9c4a9ca95a0bb4ed7c67edaf392d4`
retained the exact same tests and fake-QUIC interface, but restored only
`udpIOImpl.SendMessage` from `9baeab73ed0073add2fba7f837d9dae721ac8576` and
removed the candidate's private helpers. Consequently `sendMessageAutoFrag`
used its original `SendMessage` fallback for every fragment. The candidate
worktree was not changed. The restored function's hash and temporary path are
in `baseline-setup.json`.

The baseline failed the fragmented-success, first/second-fragment-error,
fragment-too-large, and concurrent accounting checks. Its small/zero payload,
initial ordinary error, serialization drop, and rejection cases passed.
The fragmented success logged four callbacks, and sixteen concurrent original
250-byte payloads logged 64 callbacks and 8000 bytes instead of 16 callbacks
and 4000 bytes. `baseline.log` records the expected test failure (exit 1).

The unchanged candidate then passed the same limited tests with `-race` on
2026-10-15 from 15:34:06 to 15:35:49 UTC. The test binary reported 1.100 seconds;
the wall window includes race-enabled compilation under the same strict
0.25 CPU limit. No race warnings occurred. Source was mounted read-only, the
network was disabled, and the shared experiment lock remained held for this
run. `candidate-race.log` contains the exact output. No network performance
load, production rebuild, restart, or deployment was performed. This confirms
the tested accounting/authorization paths and shared-IO concurrency behavior,
not end-to-end delivery or whole-server race freedom.

Before deployment, review the difference in callback granularity and run the
normal release checks. After any future deployment, compare fresh, same-epoch
API RX deltas against H1 target-read and enqueue payload deltas while recording
serialization/send errors and observation gaps. A drop from about 2x to about
1x accounted bytes would validate the accounting fix; it must not be reported
as a reduction in actual delivered traffic or as improved video performance.
