#!/usr/bin/env python3
"""Backfill api_key_name in Qdrant from PostgreSQL Generation table."""
import os
import sys

# Load .env BEFORE importing config/db
from pathlib import Path
_env_path = Path(__file__).resolve().parent / ".env"
if _env_path.exists():
    for line in _env_path.read_text(encoding="utf-8-sig").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        if k and k not in os.environ:
            v = v.strip().strip("\"'")
            os.environ[k] = v

from db import init_db, get_session, Generation
init_db()
from qdrant_client import QdrantClient

QDRANT_URL = os.environ.get("QDRANT_URL", "http://localhost:6335")
COLLECTION = os.environ.get("QDRANT_COLLECTION", "Polza_user_logs")

client = QdrantClient(url=QDRANT_URL)
session = get_session()
offset = None
total_scanned = 0
total_updated = 0

while True:
    records, next_offset = client.scroll(
        collection_name=COLLECTION,
        limit=200,
        offset=offset,
        with_payload=True,
        with_vectors=False,
    )
    if not records:
        break

    need_update = []
    for r in records:
        if not r.payload.get("api_key_name"):
            gen_id = r.payload.get("generation_id", "")
            if gen_id:
                need_update.append((r.id, gen_id))

    total_scanned += len(records)

    for point_id, gen_id in need_update:
        gen = session.query(Generation.source_key_name).filter(Generation.id == gen_id).first()
        if gen and gen[0]:
            client.set_payload(
                collection_name=COLLECTION,
                payload={"api_key_name": gen[0]},
                points=[point_id],
            )
            total_updated += 1

    print(f"  scanned={total_scanned}, updated={total_updated}", flush=True)

    if next_offset is None:
        break
    offset = next_offset

print(f"DONE. Scanned: {total_scanned}, Updated: {total_updated}")
session.close()
