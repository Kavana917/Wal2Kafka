import json
import os
from datetime import datetime, timezone

import psycopg2
import requests
from flask import Flask, jsonify, request, send_from_directory
from kafka import KafkaConsumer, TopicPartition
from psycopg2.extras import RealDictCursor


app = Flask(__name__, static_folder="static")

POSTGRES = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "cdc_demo"),
    "user": os.getenv("POSTGRES_USER", "cdc_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "cdc_password"),
    "connect_timeout": 2,
}
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "wal2kafka.public.users")
CONNECT_URL = os.getenv("CONNECT_URL", "http://localhost:8083").rstrip("/")
ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200").rstrip("/")
ES_INDEX = "wal2kafka.public.users"
CONNECTORS = ("postgres-cdc", "elasticsearch-sink")


def db_connection():
    return psycopg2.connect(**POSTGRES)


def get_users():
    with db_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT id, name, email, age, updated_at FROM public.users ORDER BY id")
        return [dict(row) for row in cur.fetchall()]


def get_elasticsearch_documents():
    response = requests.get(
        f"{ELASTICSEARCH_URL}/{ES_INDEX}/_search",
        params={"size": 200, "sort": "id:asc"},
        timeout=2,
    )
    if response.status_code == 404:
        return []
    response.raise_for_status()
    return [hit["_source"] for hit in response.json().get("hits", {}).get("hits", [])]


def connector_status(name):
    response = requests.get(f"{CONNECT_URL}/connectors/{name}/status", timeout=2)
    if response.status_code == 404:
        return {"state": "NOT_REGISTERED", "tasks": []}
    response.raise_for_status()
    payload = response.json()
    return {
        "state": payload.get("connector", {}).get("state", "UNKNOWN"),
        "tasks": [task.get("state", "UNKNOWN") for task in payload.get("tasks", [])],
    }


def service_statuses():
    statuses = {}
    try:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.users")
            statuses["postgres"] = {"state": "UP", "rows": cur.fetchone()[0]}
    except Exception as exc:  # report individual dependencies without hiding the rest
        statuses["postgres"] = {"state": "DOWN", "detail": str(exc)}

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            enable_auto_commit=False,
            request_timeout_ms=2500,
            api_version_auto_timeout_ms=2500,
        )
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC)
        consumer.close()
        statuses["kafka"] = {
            "state": "UP" if partitions else "WAITING",
            "partitions": len(partitions or []),
        }
    except Exception as exc:
        statuses["kafka"] = {"state": "DOWN", "detail": str(exc)}

    try:
        statuses["source_connector"] = connector_status(CONNECTORS[0])
    except Exception as exc:
        statuses["source_connector"] = {"state": "DOWN", "detail": str(exc)}

    try:
        statuses["sink_connector"] = connector_status(CONNECTORS[1])
    except Exception as exc:
        statuses["sink_connector"] = {"state": "DOWN", "detail": str(exc)}

    try:
        response = requests.get(ELASTICSEARCH_URL, timeout=2)
        response.raise_for_status()
        count_response = requests.get(f"{ELASTICSEARCH_URL}/{ES_INDEX}/_count", timeout=2)
        count = count_response.json().get("count", 0) if count_response.ok else 0
        statuses["elasticsearch"] = {"state": "UP", "documents": count}
    except Exception as exc:
        statuses["elasticsearch"] = {"state": "DOWN", "detail": str(exc)}

    return statuses


def read_recent_events(limit=30):
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        enable_auto_commit=False,
        consumer_timeout_ms=800,
        request_timeout_ms=3000,
        api_version_auto_timeout_ms=2500,
        key_deserializer=lambda raw: json.loads(raw.decode("utf-8")) if raw else None,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")) if raw else None,
    )
    try:
        partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
        if not partitions:
            return []
        topic_partitions = [TopicPartition(KAFKA_TOPIC, number) for number in partitions]
        consumer.assign(topic_partitions)
        starts = consumer.beginning_offsets(topic_partitions)
        ends = consumer.end_offsets(topic_partitions)
        per_partition = max(1, limit // max(1, len(topic_partitions)))
        for tp in topic_partitions:
            consumer.seek(tp, max(starts[tp], ends[tp] - per_partition))

        records = []
        for batch in consumer.poll(timeout_ms=900, max_records=limit).values():
            for record in batch:
                records.append({
                    "topic": record.topic,
                    "partition": record.partition,
                    "offset": record.offset,
                    "timestamp": datetime.fromtimestamp(
                        record.timestamp / 1000, tz=timezone.utc
                    ).isoformat() if record.timestamp else None,
                    "key": record.key,
                    "value": record.value,
                })
        return sorted(
            records,
            key=lambda item: (item["timestamp"] or "", item["partition"], item["offset"]),
            reverse=True,
        )[:limit]
    finally:
        consumer.close()


def validate_user(payload):
    name = str(payload.get("name", "")).strip()
    email = str(payload.get("email", "")).strip()
    age_value = payload.get("age")
    if not name or len(name) > 100:
        raise ValueError("Name is required and must be 100 characters or fewer.")
    if not email or len(email) > 150 or "@" not in email:
        raise ValueError("Enter a valid email address (150 characters or fewer).")
    if age_value in (None, ""):
        age = None
    else:
        try:
            age = int(age_value)
        except (TypeError, ValueError) as exc:
            raise ValueError("Age must be a whole number or blank.") from exc
        if age < 0 or age > 150:
            raise ValueError("Age must be between 0 and 150.")
    return name, email, age


@app.get("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.get("/api/overview")
def overview():
    result = {"services": service_statuses()}
    try:
        result["postgres_users"] = get_users()
    except Exception as exc:
        result["postgres_users"] = []
        result["postgres_error"] = str(exc)
    try:
        result["elasticsearch_users"] = get_elasticsearch_documents()
    except Exception as exc:
        result["elasticsearch_users"] = []
        result["elasticsearch_error"] = str(exc)
    return jsonify(result)


@app.get("/api/events")
def events():
    try:
        return jsonify({"events": read_recent_events()})
    except Exception as exc:
        return jsonify({"events": [], "error": str(exc)}), 503


@app.post("/api/users")
def create_user():
    try:
        name, email, age = validate_user(request.get_json(silent=True) or {})
        with db_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO public.users (name, email, age) VALUES (%s, %s, %s) "
                "RETURNING id, name, email, age, updated_at",
                (name, email, age),
            )
            user = dict(cur.fetchone())
        return jsonify({"user": user}), 201
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except psycopg2.errors.UniqueViolation:
        return jsonify({"error": "That email address already exists."}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@app.put("/api/users/<int:user_id>")
def update_user(user_id):
    try:
        name, email, age = validate_user(request.get_json(silent=True) or {})
        with db_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "UPDATE public.users SET name=%s, email=%s, age=%s, updated_at=CURRENT_TIMESTAMP "
                "WHERE id=%s RETURNING id, name, email, age, updated_at",
                (name, email, age, user_id),
            )
            user = cur.fetchone()
            if user is None:
                return jsonify({"error": "User not found."}), 404
        return jsonify({"user": dict(user)}), 200
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except psycopg2.errors.UniqueViolation:
        return jsonify({"error": "That email address already exists."}), 409
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@app.delete("/api/users/<int:user_id>")
def delete_user(user_id):
    try:
        with db_connection() as conn, conn.cursor() as cur:
            cur.execute("DELETE FROM public.users WHERE id=%s RETURNING id", (user_id,))
            deleted = cur.fetchone()
        if deleted is None:
            return jsonify({"error": "User not found."}), 404
        return jsonify({"deleted_id": deleted[0]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 503


@app.post("/api/connectors/<name>/<action>")
def control_connector(name, action):
    if name not in CONNECTORS or action not in ("pause", "resume"):
        return jsonify({"error": "Unknown connector action."}), 404
    try:
        response = requests.put(f"{CONNECT_URL}/connectors/{name}/{action}", timeout=4)
        if response.status_code not in (200, 202, 204):
            return jsonify({"error": response.text or f"Connect API returned {response.status_code}."}), 502
        return jsonify({"name": name, "action": action, "accepted": True})
    except requests.RequestException as exc:
        return jsonify({"error": str(exc)}), 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3000, threaded=True)
