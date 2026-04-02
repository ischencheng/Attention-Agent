#!/usr/bin/env python3
"""Write to Kafka topics (annotations, decision_packet). Sink consumer persists to DB.

Usage:
    python skills/db_write.py '{"topic": "annotations", "payload": {"agent_id": "claude-code", "theme": "TSLA Q1"}}'
    python skills/db_write.py '{"topic": "decision_packet", "payload": {"agent_id": "claude-code", "signal": "bullish"}}'
"""
from __future__ import annotations

import json
import os
import sys

import psycopg2
from confluent_kafka import Producer

ALLOWED_TOPICS = {"annotations", "decision_packet"}

KAFKA_BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "localhost:9092,localhost:9093,localhost:9094"
)

DB_SETTINGS = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "bluesky"),
    "user": os.environ.get("DB_USER", "bluesky"),
    "password": os.environ.get("DB_PASSWORD", "bluesky"),
}


def validate_ticket_ids(ticket_ids: list[int]) -> tuple[list[int], list[int]]:
    """Check which attention_ticket_ids actually exist in DB. Returns (valid, invalid)."""
    if not ticket_ids:
        return [], []
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM attention_tickets WHERE id = ANY(%s)",
                (ticket_ids,),
            )
            found = {row[0] for row in cur.fetchall()}
        conn.close()
    except Exception as e:
        # If DB is unreachable, warn but don't block
        print(json.dumps({"warning": f"Could not validate ticket IDs (DB error: {e}). Proceeding anyway."}),
              file=sys.stderr)
        return ticket_ids, []
    valid = [tid for tid in ticket_ids if tid in found]
    invalid = [tid for tid in ticket_ids if tid not in found]
    return valid, invalid


def validate_annotation_ids(annotation_ids: list[int]) -> tuple[list[int], list[int]]:
    """Check which annotation IDs actually exist in DB. Returns (valid, invalid)."""
    if not annotation_ids:
        return [], []
    try:
        conn = psycopg2.connect(**DB_SETTINGS)
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM annotations WHERE id = ANY(%s)",
                (annotation_ids,),
            )
            found = {row[0] for row in cur.fetchall()}
        conn.close()
    except Exception as e:
        print(json.dumps({"warning": f"Could not validate annotation IDs (DB error: {e}). Proceeding anyway."}),
              file=sys.stderr)
        return annotation_ids, []
    valid = [aid for aid in annotation_ids if aid in found]
    invalid = [aid for aid in annotation_ids if aid not in found]
    return valid, invalid


def produce_message(topic: str, payload: dict) -> dict:
    if topic not in ALLOWED_TOPICS:
        return {"error": f"Topic '{topic}' not allowed. Use: {sorted(ALLOWED_TOPICS)}"}

    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "zstd",
    })

    result = {"status": "pending"}
    delivery_error = [None]

    def on_delivery(err, msg):
        if err:
            delivery_error[0] = str(err)
            result["status"] = "error"
            result["error"] = str(err)
        else:
            result["status"] = "ok"
            result["topic"] = msg.topic()
            result["partition"] = msg.partition()
            result["offset"] = msg.offset()

    value = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    producer.produce(
        topic=topic,
        value=value,
        on_delivery=on_delivery,
    )
    producer.flush(timeout=10)

    if delivery_error[0]:
        result["status"] = "error"
        result["error"] = delivery_error[0]

    return result


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python db_write.py '<json_params>'"}))
        sys.exit(1)

    try:
        params = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}))
        sys.exit(1)

    topic = params.get("topic", "")
    payload = params.get("payload", {})

    if not isinstance(payload, dict) or not payload:
        print(json.dumps({"error": "payload must be a non-empty JSON object"}))
        sys.exit(1)

    # Validate foreign key references before writing
    if topic == "annotations":
        ticket_ids = payload.get("attention_ticket_ids", [])
        if ticket_ids:
            valid, invalid = validate_ticket_ids(ticket_ids)
            if invalid:
                print(json.dumps({
                    "error": f"attention_ticket_ids {invalid} do not exist in DB. "
                             f"Valid IDs: {valid}. Query tickets first with db_query.py.",
                }))
                sys.exit(1)

    if topic == "decision_packet":
        # Validate framework_analysis completeness (hard gate)
        fa = payload.get("framework_analysis", {})
        required_fa = [
            "traversability", "barbell_position", "moat",
            "cycle_position", "prohibitions_check", "technical_analysis",
        ]
        missing_fa = [f for f in required_fa if not fa.get(f)]
        if missing_fa:
            print(json.dumps({
                "error": f"decision_packet REJECTED: framework_analysis missing required fields: {missing_fa}. "
                         f"You MUST complete all 7 steps (including mrbrain_invest.py and price_history.py) before writing a decision."
            }))
            sys.exit(1)

        annotation_ids = payload.get("related_annotations", [])
        if annotation_ids:
            valid, invalid = validate_annotation_ids(annotation_ids)
            if invalid:
                print(json.dumps({
                    "error": f"related_annotations {invalid} do not exist in DB. "
                             f"Valid IDs: {valid}. Query annotations first with db_query.py.",
                }))
                sys.exit(1)

    try:
        result = produce_message(topic, payload)
        print(json.dumps(result, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
