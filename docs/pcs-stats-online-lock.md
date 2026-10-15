# Release the statistics lock before writing an online response

`trafficStatsServerImpl.getOnline` held the shared statistics read lock until
`http.ResponseWriter.Write` returned. `LogTraffic` takes the same lock exclusively
for every authentication ID, so one slow management response could stop traffic
accounting across otherwise independent QUIC connections. The production API is
loopback-only; this is a reproduced concurrency defect, not a claim that it caused
the reported production video slowdown.

The change releases the lock after the existing JSON snapshot is encoded, before
headers or body are written. It preserves response contents, authentication,
counter semantics, and locking during map serialization. It adds no buffering,
goroutines, deadlines, or protocol changes.

`TestOnlineSlowReaderDoesNotBlockTraffic` holds a response write open while an
unrelated user's `LogTraffic` and `LogOnlineState` must complete, then verifies the
response still contains the earlier snapshot. On the baseline, the independent
traffic operation does not finish within one second. With the fix, the package
passes five repeated race-enabled runs (reported package time 1.095 seconds).

Validation on 2026-10-15 used Go 1.25.6, `GOMAXPROCS=1`, `go test -p 1
-mod=readonly -race -count=5 -timeout=60s ./trafficlogger`, in a container capped at
0.25 CPU and 384 MiB memory. The baseline failure was verified separately with the
same test and toolchain. This does not establish a throughput improvement under
ordinary fast API readers, nor remove the lock held during JSON encoding.
