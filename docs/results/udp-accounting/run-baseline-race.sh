#!/bin/sh
set -eu
root=/root/proxy-source-lab/hysteria-udp-accounting
out="$root/docs/results/udp-accounting"
baseline=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["temporary_baseline_source"])' "$out/baseline-setup.json")
exec 9>/run/lock/pcs-proxy-experiment.lock
flock -n 9
run_check() {
  name=$1
  source_path=$2
  race_flag=$3
  printf 'start_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" > "$out/$name.log"
  set +e
  docker run --rm --name "pcs-hy2-accounting-$name" --network none --cpus .25 --memory 512m --memory-swap 768m \
    -e GOMAXPROCS=1 -e GOMEMLIMIT=384MiB -e GOTOOLCHAIN=local -e GOWORK=off -e CGO_ENABLED=1 \
    -e GOCACHE=/go-cache -e GOMODCACHE=/go-mod-cache -e GOPROXY=off \
    -v "$source_path:/source:ro" \
    -v /root/proxy-source-lab/anytls-audit/go-cache:/go-cache \
    -v /root/proxy-source-lab/anytls-audit/go-mod-cache:/go-mod-cache \
    -w /source/core pcs-proxy-test:go1.25.6-race-20261015 \
    sh -c 'go test -p 1 -mod=readonly -count=1 ${1:+-race} -run "^TestUDPPayloadAccounting" -v ./server' sh "$race_flag" >> "$out/$name.log" 2>&1
  result=$?
  set -e
  printf 'end_utc=%s\nexit_status=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$result" >> "$out/$name.log"
  cat "$out/$name.log"
  if [ "$name" = baseline ]; then
    [ "$result" -eq 1 ]
    grep -q 'calls=4 order=' "$out/$name.log"
  else
    [ "$result" -eq 0 ]
  fi
}
case "${1:-all}" in
  all) run_check baseline "$baseline" ''; run_check candidate-race "$root" race ;;
  candidate-race) run_check candidate-race "$root" race ;;
  *) exit 2 ;;
esac
