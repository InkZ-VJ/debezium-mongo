# Debezium MongoDB to Kafka

A local proof of concept. MongoDB change events flow through Debezium (on Kafka Connect) into Kafka. Only the collections named in the connector config are captured. Everything runs from `compose.yaml` under Podman.

## Prerequisites

- A Podman machine with 4 GiB of memory: `podman machine init --cpus 6 --memory 4096`, then `podman machine start`. The stack peaks at about 1.9 GiB in use. The default 2 GiB machine is too tight, and the kernel kills Connect first (`connect` shows `Exited (137)`).
- `brew install podman-compose`
- `curl` and `jq`

## Run

```sh
podman compose up -d
scripts/register-connector.sh
scripts/verify.sh
```

`register-connector.sh` is safe to run any number of times. `verify.sh` seeds data, registers the connector, makes live changes, reads the topics, and prints PASS or FAIL. It can run on a fresh or an already running stack.

## Watch a topic

```sh
podman compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic mongo.inventory.customers --from-beginning
```

From the host, Kafka is also reachable at `localhost:9094`, and Connect's REST API is at `http://localhost:8083`.

## Kafka UI

Redpanda Console runs at http://localhost:8080. It shows the topics, decodes the JSON events, and lists the `mongo-inventory` connector with its task state under Connect. Its settings are in `console/config.yaml`.

Kafka has no data volume. Recreating the `kafka` container clears every topic, including the one that stores the connector registration. `podman compose up --force-recreate console` does this too, because it also recreates the services the console depends on. After that, rerun `scripts/verify.sh` or `scripts/register-connector.sh`.

## Connect to MongoDB

- From the host: `mongodb://localhost:27017/inventory?directConnection=true`
- From another container on the compose network: `mongodb://mongo:27017/inventory?replicaSet=rs0`

From the host, keep `directConnection=true`. The replica set advertises its member as `mongo:27017`, which does not resolve outside the containers. MongoDB runs without authentication.

## Change the captured collections

`connectors/mongo-inventory.json` is the only place the collection list lives. Edit `collection.include.list`, then rerun `scripts/register-connector.sh`. The connector name comes from the file name.

Adding a collection does not snapshot the documents already in it. The connector resumes from its last stored offset, so the new topic receives changes made after that offset, including some made while the collection was still excluded. To backfill existing documents, use a Debezium incremental snapshot signal. Removing a collection stops new events for it, and its topic stays in Kafka until you delete it.

## Tear down

```sh
podman compose down -v
```

This removes the containers and the MongoDB data volume.
