#!/usr/bin/env bash
set -euo pipefail

rate="${1:-30000}"
duration="${2:-60}"

if ! [[ "${rate}" =~ ^[1-9][0-9]*$ && "${duration}" =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: $0 [positive-events-per-second] [positive-duration-seconds]" >&2
  exit 2
fi

echo "Running PulseDrop benchmark at ${rate} events/sec for ${duration} seconds."
docker compose stop generator >/dev/null 2>&1 || true
baseline="$(docker compose exec -T clickhouse clickhouse-client \
  --user pulsedrop --password pulsedrop \
  --query "SELECT count() FROM pulsedrop.playback_events FINAL")"
started_epoch="$(date +%s)"

docker compose run --rm \
  --no-deps \
  -e "EVENTS_PER_SECOND=${rate}" \
  -e "GENERATOR_DURATION_SECONDS=${duration}" \
  generator

expected="$((rate * duration))"
deadline="$((started_epoch + duration + 180))"
stored="${baseline}"

echo "Waiting for Flink and ClickHouse to ingest ${expected} events..."
while (( $(date +%s) < deadline )); do
  stored="$(docker compose exec -T clickhouse clickhouse-client \
    --user pulsedrop --password pulsedrop \
    --query "SELECT count() FROM pulsedrop.playback_events FINAL")"
  if (( stored - baseline >= expected )); then
    break
  fi
  sleep 5
done

ingested="$((stored - baseline))"
if (( ingested < expected )); then
  echo "Timed out after ingesting ${ingested} of ${expected} generated events." >&2
  exit 1
fi

latencies="$(mktemp)"
trap 'rm -f "${latencies}"' EXIT
for _ in {1..20}; do
  curl --fail --silent --output /dev/null \
    --write-out '%{time_total}\n' http://localhost:8080/api/v1/overview >>"${latencies}"
done
p95_seconds="$(sort -n "${latencies}" | sed -n '19p')"
p95_millis="$(awk -v seconds="${p95_seconds}" 'BEGIN { printf "%.1f", seconds * 1000 }')"
elapsed="$(( $(date +%s) - started_epoch ))"

echo "Benchmark result"
echo "  target events: ${expected}"
echo "  stored events: ${ingested}"
echo "  end-to-end catch-up time: ${elapsed}s"
echo "  API query latency P95 (20 requests): ${p95_millis} ms"
echo "  API overview: $(curl --fail --silent http://localhost:8080/api/v1/overview)"
echo "Use the Flink dashboard at http://localhost:8081 for backpressure and checkpoint evidence."
