#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CONFIG="${1:-$ROOT/connectors/mongo-inventory.json}"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
NAME="$(basename "$CONFIG" .json)"

echo "Waiting for Kafka Connect at $CONNECT_URL"
until curl -fsS "$CONNECT_URL/" >/dev/null 2>&1; do sleep 2; done

echo "Applying connector '$NAME' from $CONFIG"
curl -fsS -X PUT -H 'Content-Type: application/json' \
  --data @"$CONFIG" "$CONNECT_URL/connectors/$NAME/config" | jq -c '{name, collections: .config["collection.include.list"]}'

echo "Waiting for connector and task to be RUNNING"
for _ in $(seq 1 60); do
  status="$(curl -fsS "$CONNECT_URL/connectors/$NAME/status" 2>/dev/null || true)"
  states="$(jq -r '[.connector.state] + [.tasks[].state] | join(" ")' <<<"${status:-{\}}" 2>/dev/null || true)"
  if [[ "$states" == *FAILED* ]]; then
    echo "FAILED: $states" >&2
    jq . <<<"$status" >&2
    exit 1
  fi
  if [[ -n "$states" ]] && [[ "$(jq '.tasks | length' <<<"$status")" -gt 0 ]] \
     && [[ -z "$(tr ' ' '\n' <<<"$states" | grep -vx RUNNING)" ]]; then
    jq -c '{name, connector: .connector.state, tasks: [.tasks[].state]}' <<<"$status"
    exit 0
  fi
  sleep 2
done
echo "Timed out waiting for RUNNING (last: ${states:-none})" >&2
exit 1
