#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
CONFIG="$ROOT/connectors/mongo-inventory.json"
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
PREFIX="$(jq -r '.["topic.prefix"]' "$CONFIG")"
INCLUDED="$(jq -r '.["collection.include.list"]' "$CONFIG" | tr ',' ' ')"
RUN="$(date +%s)"
FAILURES=0
export PODMAN_COMPOSE_WARNING_LOGS=false

mongo()  { podman compose exec -T mongo mongosh --quiet --eval "$1"; }
kafka()  { podman compose exec -T kafka "$@"; }
consume() { kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 \
  --topic "$1" --from-beginning --timeout-ms 5000 --property print.key=true 2>/dev/null || true; }
# Emits one line per event for this run's document: "<op>".
ops_for() { consume "$1" | jq -R -r --arg id "$2" \
  'split("\t") | select(.[0] | contains($id)) | .[1] | fromjson? | select(. != null) | .op'; }
check() {
  if [[ "$1" == ok ]]; then echo "  PASS  $2"; else echo "  FAIL  $2"; FAILURES=$((FAILURES + 1)); fi
}

echo "Waiting for the stack to be healthy"
until curl -fsS "$CONNECT_URL/" >/dev/null 2>&1; do sleep 2; done
until [[ "$(mongo 'rs.status().myState' 2>/dev/null | tail -1)" == 1 ]]; do sleep 2; done
until kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do sleep 2; done

echo "Seeding inventory.customers, inventory.orders and inventory.audit_logs (run $RUN)"
mongo "
  const d = db.getSiblingDB('inventory');
  d.customers.insertOne({_id: 'seed-$RUN', name: 'Seed Customer'});
  d.orders.insertOne({_id: 'seed-$RUN', total: 10});
  d.audit_logs.insertOne({_id: 'seed-$RUN', msg: 'seed'});
" >/dev/null

has_snapshot() { consume "$PREFIX.inventory.customers" | jq -R -e 'split("\t")[1] | fromjson? | select(.op == "r")' >/dev/null; }

# The initial snapshot runs once per offset. A connector registered earlier against empty
# collections never emitted op r, so reset it to make this run prove the snapshot path.
NAME="$(basename "$CONFIG" .json)"
if curl -fsS "$CONNECT_URL/connectors/$NAME" >/dev/null 2>&1 && ! has_snapshot; then
  echo "Connector exists without a snapshot event, resetting its offsets"
  curl -fsS -X PUT "$CONNECT_URL/connectors/$NAME/stop" >/dev/null
  until [[ "$(curl -fsS "$CONNECT_URL/connectors/$NAME/status" | jq -r .connector.state)" == STOPPED ]]; do sleep 1; done
  curl -fsS -X DELETE "$CONNECT_URL/connectors/$NAME/offsets" >/dev/null
  curl -fsS -X DELETE "$CONNECT_URL/connectors/$NAME" >/dev/null
fi

echo "Registering connector"
"$ROOT/scripts/register-connector.sh" "$CONFIG"

echo "Live insert, update, delete on inventory.customers; insert on orders; writes to audit_logs"
mongo "
  const d = db.getSiblingDB('inventory');
  d.customers.insertOne({_id: 'live-$RUN', name: 'Live Customer', tier: 'silver'});
  d.customers.updateOne({_id: 'live-$RUN'}, {\$set: {tier: 'gold'}});
  d.customers.deleteOne({_id: 'live-$RUN'});
  d.orders.insertOne({_id: 'live-$RUN', total: 99});
  d.audit_logs.insertOne({_id: 'live-$RUN', msg: 'live'});
  d.audit_logs.updateOne({_id: 'live-$RUN'}, {\$set: {msg: 'changed'}});
" >/dev/null

echo "Consuming from Kafka"
for _ in $(seq 1 12); do
  live_ops="$(ops_for "$PREFIX.inventory.customers" "live-$RUN" | sort -u | tr '\n' ' ')"
  [[ "$live_ops" == *c*d*u* ]] && break
done
topics="$(kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list)"

echo "Assertions"
for coll in $INCLUDED; do
  topic="$PREFIX.$coll"
  grep -qx "$topic" <<<"$topics" && check ok "topic $topic exists" || check fail "topic $topic exists"
  [[ -n "$(consume "$topic")" ]] && check ok "topic $topic has events" || check fail "topic $topic has events"
done
seed_ops="$(ops_for "$PREFIX.inventory.customers" "seed-$RUN" | sort -u | tr -d '\n')"
[[ "$seed_ops" == [rc] || "$seed_ops" == cr ]] \
  && check ok "customers seed doc captured (ops: $seed_ops)" \
  || check fail "customers seed doc captured"
[[ -n "$(ops_for "$PREFIX.inventory.orders" "seed-$RUN")" ]] \
  && check ok "orders seed doc captured" || check fail "orders seed doc captured"
has_snapshot \
  && check ok "customers topic holds a snapshot (op r) event" || check fail "customers topic holds a snapshot (op r) event"
for op in c u d; do
  [[ " $live_ops" == *" $op "* ]] && check ok "live customers op '$op' captured" || check fail "live customers op '$op' captured"
done
[[ -n "$(ops_for "$PREFIX.inventory.orders" "live-$RUN")" ]] \
  && check ok "live orders insert captured" || check fail "live orders insert captured"
grep -q 'audit_logs' <<<"$topics" && check fail "no topic for audit_logs" || check ok "no topic for audit_logs"
state="$(curl -fsS "$CONNECT_URL/connectors/$NAME/status" 2>/dev/null | jq -r '[.connector.state] + [.tasks[].state] | join(" ")' || echo "connect unreachable")"
[[ "$state" == "RUNNING RUNNING" ]] && check ok "connector still RUNNING" || check fail "connector still RUNNING (got: $state)"

echo
if [[ "$FAILURES" -eq 0 ]]; then echo "RESULT: PASS"; else echo "RESULT: FAIL ($FAILURES failed)"; exit 1; fi
