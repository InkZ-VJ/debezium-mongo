# Debezium MongoDB to Confluent Cloud

One Kafka Connect worker runs Debezium and streams changes from the Thaimart UAT MongoDB Atlas cluster into Confluent Cloud. It captures seven collections:

- `product-api`: `products`, `productVariants`, `categories`
- `shop-api`: `shops`
- `order-api`: `orders`
- `return-api`: `returnOrders`
- `review-api`: `reviewSummaries`

Each collection gets one compacted topic named `cdc.<database>.<collection>`, for example `cdc.order-api.orders`. On first start the connector snapshots every document. After that it follows the change stream.

The VM runs only the worker. MongoDB and Kafka are managed services. `compose.yaml` defines the worker, and `connectors/thaimart-mongo.json` defines the connector.

## Before you start

You need these four things before you create the VM:

- **Network access to Atlas.** The connection string uses the cluster's `-pri` hostname, which resolves only from a network that is peered with the Atlas project. Put the VM in that network. Confluent Cloud runs in GCP `asia-southeast1`, so a VM in that region keeps latency low.
- **Outbound access to Confluent Cloud** on port 9092. The VM needs no inbound ports except SSH.
- **An Atlas database user** with read access to the five databases and permission to list databases. The built-in `readAnyDatabase` role covers both.
- **A Confluent Cloud API key** whose service account can do the following:
	- Create, describe, and write topics with the prefixes `cdc.` and `__debezium-heartbeat.`
	- Create, read, and write the worker's internal topics `dbz-connect-configs`, `dbz-connect-offsets`, and `dbz-connect-status`
	- Read the consumer group `dbz-connect`

## Size the VM

Use a Linux VM with 2 vCPUs and 4 GB of memory. The worker's Java heap goes up to 2 GB (`KAFKA_HEAP_OPTS` in `compose.yaml`), and the OS needs the rest. These steps assume Ubuntu 24.04 or Debian 12.

On Fedora, RHEL, or another distribution with SELinux enforcing, change the volume in `compose.yaml` to `./secrets:/secrets:ro,Z`. Without that change, the worker cannot read the Mongo connection string.

## Set up the VM

1. Install Podman and the tools the scripts use:

	```sh
	sudo apt-get update
	sudo apt-get install -y podman podman-compose git curl jq
	```

2. Let containers start again after a reboot. Rootless Podman restarts `restart: unless-stopped` containers only through this service:

	```sh
	sudo loginctl enable-linger "$USER"
	systemctl --user enable --now podman-restart.service
	```

3. Clone the repository:

	```sh
	git clone <repo-url> debezium-mongo
	cd debezium-mongo
	```

4. Create `.env` from the example, then fill in the Confluent Cloud bootstrap server, API key, and API secret. Keep `DBZ_VERSION` at the full tag so that a later pull cannot change the version:

	```sh
	cp .env.example .env
	chmod 600 .env
	```

	Git ignores `.env` and `secrets/`, so the credentials stay on the VM.

5. Create `secrets/mongo.properties` with the Atlas connection string under the key `uri`. Write it with an editor rather than `echo`, so that the password stays out of your shell history:

	```properties
	uri=mongodb+srv://<user>:<password>@<cluster>-pri.<id>.mongodb.net/?appName=uat-debezium-cdc&authSource=admin&readPreference=secondary
	```

6. Give the file to the container user. The worker runs as uid 1001 inside the container. Under rootless Podman a file you own appears as root inside the container, and uid 1001 cannot read it:

	```sh
	chmod 600 secrets/mongo.properties
	podman unshare chown 1001:1001 secrets/mongo.properties
	```

7. Start the worker and wait until it reports `healthy`:

	```sh
	podman compose up -d
	podman ps --filter name=kafka-connect --format '{{.Status}}'
	```

The worker stores connector configs and offsets in Confluent Cloud, not on the VM. If a connector was already registered from another machine, the worker picks it up and resumes from the last offset. It does not snapshot again.

## Run one worker only

All workers with `GROUP_ID: dbz-connect` join one Connect cluster. A second worker on a laptop or another VM joins that cluster, and REST calls between the two fail because `kafka-connect` resolves only inside each machine. Before you start the VM, stop every other copy:

```sh
podman compose down
```

## Register the connector

Apply the connector config. The script uses the file name as the connector name, sends the config with `PUT`, and waits until the task is `RUNNING`. You can run it any number of times:

```sh
scripts/register-connector.sh connectors/thaimart-mongo.json
```

`RUNNING` does not mean the snapshot passed. A bad document can fail the task minutes later. Check that the snapshot finished:

```sh
podman logs kafka-connect 2>&1 | grep -E 'Finished snapshotting|Snapshot completed|ERROR'
curl -s localhost:8083/connectors/thaimart-mongo/status | jq '{connector: .connector.state, tasks: [.tasks[].state]}'
curl -s localhost:8083/connectors/thaimart-mongo/offsets | jq '.offsets[].offset'
```

The snapshot is done when the log shows `Snapshot completed`, the task stays `RUNNING`, and the offset no longer contains `"initsync": true`. The offset updates once a minute, so the last check can lag the log.

## Reach the REST API from your laptop

Port 8083 listens on `127.0.0.1` only, because the Connect REST API has no authentication. Open an SSH tunnel to use it from your laptop:

```sh
ssh -N -L 8083:localhost:8083 <vm>
```

Then `curl localhost:8083/connectors` on your laptop talks to the VM.

## Change the connector

Edit `connectors/thaimart-mongo.json`, then run `scripts/register-connector.sh connectors/thaimart-mongo.json` again. Connect restarts the task with the new config.

Adding a collection to `collection.include.list` does not snapshot documents that already exist. To backfill them, send a Debezium incremental snapshot signal.

## Restart a failed task

A failed task stays failed until you restart it:

```sh
curl -s localhost:8083/connectors/thaimart-mongo/status | jq -r '.tasks[].trace' | head -40
curl -X POST 'localhost:8083/connectors/thaimart-mongo/restart?includeTasks=true'
```

If the snapshot had not finished, the restart snapshots every collection again. Topics are compacted by document key, so a repeat snapshot replaces earlier records rather than adding to them.

## Handle arrays that mix types

The `unwrap` transform turns each Mongo document into a typed record. By default every element of an array must have the same type. One document that breaks this rule fails the task with one of these errors:

- `DataException: Not a struct schema`
- `Array elements with different Bson types are not supported`

Two collections break the rule today:

- `orders`, where an array nested inside an array of documents holds both documents and strings (the exact field is not yet known)
- `returnOrders`, where the history array stores `attachments` as a string in some entries and as an array in others

These topics go through a second transform, `unwrapMixed`, which sets `array.encoding` to `document`. The predicate `hasMixedArrays` lists them. If another collection fails the same way, add its topic to `predicates.hasMixedArrays.pattern` and register the connector again.

`array.encoding: document` changes the message shape. On `cdc.order-api.orders` and `cdc.return-api.returnOrders`, every array arrives as an object keyed by position:

```text
plain array:        "items": [ {...}, {...} ]
document encoding:  "items": { "_0": {...}, "_1": {...} }
```

The other topics keep plain JSON arrays. To return a topic to plain arrays, fix the data in the owning service so that each array holds one type, then remove the topic from the pattern.

## Message format

Each message value is the document as plain JSON with no schema envelope. A `ReplaceField` transform per collection keeps only the fields listed in `transforms.<name>.include`. Every message also carries:

- `__op`: `c` for create, `u` for update, `r` for a snapshot read
- `__source_ts_ms`: the time of the change in MongoDB, in milliseconds

A delete sends a tombstone, which is a record with the document key and a null value. Compaction then removes that key from the topic.

## Stop the worker

```sh
podman compose down
```

This stops the container. The connector config and offsets stay in Confluent Cloud, so the next `podman compose up -d` resumes where the worker stopped.
