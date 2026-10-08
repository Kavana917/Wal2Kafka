# Wal2Kafka

Wal2Kafka is a free, local Change Data Capture (CDC) demonstration built with Docker Compose. It captures changes to a PostgreSQL table from the database write-ahead log (WAL), streams them through Debezium and Apache Kafka, and indexes the resulting records in Elasticsearch.

The project is intended as a reproducible learning and portfolio example for CDC, event-driven architecture, Kafka Connect, and search-oriented data synchronization. All services run locally; no cloud account or paid service is required.

## Architecture

```text
PostgreSQL 16
    │ logical WAL
    ▼
Debezium 3.3 on Kafka Connect
    │ CDC events
    ▼
Apache Kafka 4.0.1 (KRaft)
    │ wal2kafka.public.users
    ▼
Confluent Elasticsearch Sink Connector 16.0.0
    │ flattened user documents
    ▼
Elasticsearch 8.15.0
```

The PostgreSQL `public.users` table is the sample source. The Debezium connector publishes its change events to `wal2kafka.public.users`. The sink unwraps each Debezium envelope, uses the row's primary key as the Elasticsearch document ID, applies inserts and updates, and removes documents when it receives Kafka tombstones for deleted rows.

## Prerequisites

- Windows with Docker Desktop using the Linux container engine
- Docker Compose v2
- PowerShell
- Enough memory for PostgreSQL, Kafka, Kafka Connect, and Elasticsearch to run together; Elasticsearch is configured with a 512 MB heap

## Start the project

From the repository directory, build the custom Kafka Connect image and start the services:

```powershell
docker compose up -d --build
```

The custom image installs Confluent's Elasticsearch Sink Connector 16.0.0 into the Debezium Connect image. Wait for Kafka Connect to become available at `http://localhost:8083`, then register both connectors:

```powershell
./register-connectors.ps1
```

The script reads `debezium-connector.json` and `elasticsearch-sink.json` and creates or updates the corresponding connectors. It can be run again after changing either configuration.

Open the interactive workflow dashboard at **http://localhost:3000**. It shows service and connector health, PostgreSQL rows beside their Elasticsearch documents, and recent Kafka messages. Use **Add a row**, **Edit**, or **Delete** to write to PostgreSQL and watch the change propagate. The connector controls can pause and resume either stage to demonstrate where the flow stops. The dashboard is a local teaching interface, not a production administration console.

## Verify the pipeline

Check that both connectors and their tasks are `RUNNING`:

```powershell
Invoke-RestMethod http://localhost:8083/connectors/postgres-cdc/status
Invoke-RestMethod http://localhost:8083/connectors/elasticsearch-sink/status
```

Inspect indexed user documents:

```powershell
Invoke-RestMethod 'http://localhost:9200/wal2kafka.public.users/_search?pretty'
```

To exercise the pipeline, insert, update, or delete a row in PostgreSQL, then query the Elasticsearch index. For example, connect to `cdc_demo` as `cdc_user` and run SQL against `public.users`; the Elasticsearch document ID matches `users.id`.

## Local endpoints and sample credentials

| Service | Local address | Demo access |
| --- | --- | --- |
| PostgreSQL | `localhost:5432` | Database `cdc_demo`, user `cdc_user`, password `cdc_password` |
| Apache Kafka | `localhost:9092` | No authentication configured |
| Kafka Connect REST API | `http://localhost:8083` | No authentication configured |
| Elasticsearch | `http://localhost:9200` | Security disabled |
| Wal2Kafka dashboard | `http://localhost:3000` | Localhost only; no login |

These settings are for a local demonstration. The dashboard has no authentication and can change database rows and connector state. Do not expose it or the other services to an untrusted network, or reuse the sample database credentials in a real deployment.

## Project files

- `docker-compose.yml` defines PostgreSQL, Kafka in KRaft mode, Kafka Connect, and Elasticsearch.
- `Dockerfile` adds the Elasticsearch sink plugin to the Debezium Connect image.
- `postgres/init.sql` creates and seeds the sample `users` table on first database initialization, and enables full replica identity so UPDATE/DELETE events can include the prior row.
- `debezium-connector.json` configures PostgreSQL WAL capture.
- `elasticsearch-sink.json` configures event flattening, key extraction, and Elasticsearch deletes.
- `register-connectors.ps1` registers or updates both connectors through the Kafka Connect REST API.
- `dashboard/` contains the local web UI and a small API that reads PostgreSQL, Kafka, Kafka Connect, and Elasticsearch, and writes sample row changes to PostgreSQL.

## Data and cleanup

PostgreSQL and Elasticsearch data are stored in named Docker volumes. `docker compose down` removes the containers but preserves the data. To remove the stack and its stored data, run:

```powershell
docker compose down -v
```
