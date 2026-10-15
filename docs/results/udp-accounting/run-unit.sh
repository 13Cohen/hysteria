#!/bin/sh
set -eu
root=/root/proxy-source-lab/hysteria-udp-accounting
out="$root/docs/results/udp-accounting"
exec 9>/run/lock/pcs-proxy-experiment.lock
flock -n 9
printf 'start_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$out/unit.log"
set +e
docker run --rm --name pcs-hy2-udp-accounting-unit --network none --cpus .25 --memory 512m --memory-swap 768m \
 -e GOMAXPROCS=1 -e GOMEMLIMIT=384MiB -e GOTOOLCHAIN=local -e GOWORK=off -e CGO_ENABLED=1 \
 -e GOCACHE=/go-cache -e GOMODCACHE=/go-mod-cache -e GOPROXY=off \
 -v "$root:/source" \
 -v /root/proxy-source-lab/anytls-audit/go-cache:/go-cache \
 -v /root/proxy-source-lab/anytls-audit/go-mod-cache:/go-mod-cache \
 -w /source/core pcs-proxy-test:go1.25.6-race-20261015 \
 sh -c 'gofmt -w server/server.go server/udp.go server/udp_accounting_test.go && go test -p 1 -mod=readonly -count=1 -run "^TestUDPPayloadAccounting" -v ./server' >> "$out/unit.log" 2>&1
status=$?
printf 'end_utc=%s\nexit_status=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$status" >> "$out/unit.log"
cat "$out/unit.log"
exit "$status"
