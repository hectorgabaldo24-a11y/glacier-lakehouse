#!/usr/bin/env bash
# Usage: scripts/put_events.sh <stream-name> [count]
# Publishes <count> JSON events (default 100) spread over 5 regions.
set -euo pipefail

stream="${1:?stream name required}"
count="${2:-100}"
regions=(us-east sa-east eu-west ap-south af-south)

for i in $(seq 1 "$count"); do
  region="${regions[$((i % 5))]}"
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  payload="{\"event_id\":\"evt-$(date +%s)-$i\",\"event_timestamp\":\"$ts\",\"region\":\"$region\"}"
  aws kinesis put-record --stream-name "$stream" --partition-key "$region" \
    --cli-binary-format raw-in-base64-out --data "$payload" >/dev/null
done
echo "sent $count events to $stream"
