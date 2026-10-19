#!/usr/bin/env python3
"""Semantic vector search over vector_embeddings table using pgvector.

Usage:
    python skills/search_vectors.py '{"query_text": "TSLA delivery numbers", "top_k": 5}'
"""
from __future__ import annotations

import json
import math
import os
import sys
from datetime import datetime

import psycopg2
import psycopg2.extras
import requests

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "port": int(os.environ.get("DB_PORT", "5432")),
    "dbname": os.environ.get("DB_NAME", "bluesky"),
    "user": os.environ.get("DB_USER", "bluesky"),
    "password": os.environ.get("DB_PASSWORD", "bluesky"),
}

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
EMBEDDING_MODEL = os.environ.get("EMBEDDING_MODEL", "gemini-embedding-001")
EMBEDDING_DIM = int(os.environ.get("EMBEDDING_DIM", "768"))


def get_embedding(text: str) -> list[float] | None:
    """Use the same embedding space as the ingestion worker."""
    if os.environ.get("EMBEDDING_PROVIDER") == "http":
        token = os.environ.get("ATTENTION_GATEWAY_TOKEN", "")
        response = requests.post(
            os.environ.get("EMBEDDING_GATEWAY_URL", "http://127.0.0.1:8765/embed"),
            json={"text": text, "dimensions": EMBEDDING_DIM},
            headers={"Authorization": f"Bearer {token}"} if token else {},
            timeout=120,
        )
        response.raise_for_status()
        result = response.json()
        values = result.get("embedding")
        if result.get("model") != EMBEDDING_MODEL:
            raise ValueError("Embedding model does not match the configured search space")
        if (not isinstance(values, list) or len(values) != EMBEDDING_DIM
                or any(type(v) not in (int, float) or not math.isfinite(v) for v in values)
                or not any(values)):
            raise ValueError("Invalid semantic query embedding")
        return values
    if not GEMINI_API_KEY:
        return None
    endpoint = f"https://generativelanguage.googleapis.com/v1beta/models/{EMBEDDING_MODEL}:embedContent"
    resp = requests.post(
        endpoint,
        headers={"Content-Type": "application/json", "x-goog-api-key": GEMINI_API_KEY},
        json={
            "model": f"models/{EMBEDDING_MODEL}",
            "content": {"parts": [{"text": text}]},
            "outputDimensionality": EMBEDDING_DIM,
        },
        timeout=15,
    )
    resp.raise_for_status()
    data = resp.json()
    values = data.get("embedding", {}).get("values")
    if isinstance(values, list):
        return [float(v) for v in values]
    return None


def search(query_text: str, top_k: int = 5) -> list[dict]:
    embedding = get_embedding(query_text)
    if embedding is None:
        return [{"error": "Failed to generate embedding. Check configured embedding provider."}]

    vec_literal = "[" + ",".join(f"{v:.10f}" for v in embedding) + "]"

    conn = psycopg2.connect(**DB_CONFIG)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    id,
                    payload,
                    created_at,
                    1 - (embedding_vec <=> %s::vector) AS similarity
                FROM vector_embeddings
                WHERE embedding_vec IS NOT NULL
                  AND payload->>'embedding_model' = %s
                ORDER BY embedding_vec <=> %s::vector
                LIMIT %s
                """,
                (vec_literal, EMBEDDING_MODEL, vec_literal, top_k),
            )
            rows = cur.fetchall()
            return [dict(r) for r in rows]
    finally:
        conn.close()


def default_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "Usage: python search_vectors.py '<json_params>'"}))
        sys.exit(1)

    try:
        params = json.loads(sys.argv[1])
    except json.JSONDecodeError as e:
        print(json.dumps({"error": f"Invalid JSON: {e}"}))
        sys.exit(1)

    query_text = params.get("query_text", "")
    if not query_text:
        print(json.dumps({"error": "query_text is required"}))
        sys.exit(1)

    top_k = min(int(params.get("top_k", 5)), 50)

    try:
        result = search(query_text, top_k)
        print(json.dumps(result, default=default_serializer, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({"error": str(e)}))
        sys.exit(1)


if __name__ == "__main__":
    main()
