#!/usr/bin/env bash
set -euo pipefail

base_url="${PULSEDROP_URL:-http://localhost:8080}"
attempts=30

for ((attempt = 1; attempt <= attempts; attempt++)); do
  if curl --fail --silent "${base_url}/health" | grep --quiet 'healthy'; then
    break
  fi
  if [[ "${attempt}" == "${attempts}" ]]; then
    echo "PulseDrop did not become healthy after ${attempts} attempts." >&2
    exit 1
  fi
  sleep 2
done

overview="$(curl --fail --silent "${base_url}/api/v1/overview")"
content="$(curl --fail --silent "${base_url}/api/v1/content")"
timeseries="$(curl --fail --silent "${base_url}/api/v1/timeseries")"

grep --quiet 'eventsPerSecond' <<<"${overview}"
grep --quiet '^\[' <<<"${content}"
grep --quiet '^\[' <<<"${timeseries}"

echo "Health, overview, content, and timeseries endpoints passed."
echo "${overview}"
