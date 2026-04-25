#!/usr/bin/env python3
"""
Backfill script: add summary, project_guess, risk_flags to existing Qdrant vectors.

Existing records only have: generation_id, user_text_snippet, topic, is_work,
api_key_name, model_used, created_at. This script adds missing fields from
PostgreSQL generation_summaries table.

Run once:
    python3 backfill_enrich_vectors.py
"""
import sys
import os
import json
import time

# Load .env
from dotenv import load_dotenv
load_dotenv()

# Bootstrap Flask app context for DB access
from config import get_session, gen_summary_get_many
from embeddings.qdrant import _get_qdrant_client, _qdrant_ensure_collection, QDRANT_COLLECTION


def main():
    client = _get_qdrant_client()
    if not client:
        print("[Backfill] Qdrant client failed")
        sys.exit(1)

    _qdrant_ensure_collection()
    print(f"[Backfill] Connected to Qdrant, collection: {QDRANT_COLLECTION}")

    # Scroll ALL records
    all_records = []
    offset = None
    batch_size = 200

    while True:
        records, next_offset = client.scroll(
            collection_name=QDRANT_COLLECTION,
            limit=batch_size,
            offset=offset,
            with_payload=True,
            with_vectors=False,
        )
        all_records.extend(records)
        if next_offset is None or not records:
            break
        offset = next_offset

    print(f"[Backfill] Total records in Qdrant: {len(all_records)}")

    # Find records missing 'summary' field
    records_needing_update = []
    gen_ids = []

    for r in all_records:
        p = r.payload
        if "summary" not in p or not p.get("summary"):
            gid = p.get("generation_id", "")
            if gid:
                records_needing_update.append(r)
                gen_ids.append(gid)

    print(f"[Backfill] Records needing enrichment: {len(records_needing_update)}")

    if not records_needing_update:
        print("[Backfill] Nothing to do!")
        return

    # Batch fetch summaries from PostgreSQL
    print(f"[Backfill] Fetching summaries from PostgreSQL...")
    summaries = gen_summary_get_many(gen_ids)
    print(f"[Backfill] Got {len(summaries)} summaries from DB")

    # Build updates using set_payload (batched)
    updated = 0
    batch_points = []

    for r in records_needing_update:
        gid = r.payload.get("generation_id", "")
        s = summaries.get(gid, {})
        if not s:
            continue

        new_payload = {}
        if s.get("summary"):
            new_payload["summary"] = s["summary"][:500]  # Keep manageable size
        if s.get("project_guess"):
            new_payload["project_guess"] = s["project_guess"]
        risk = s.get("risk_flags", [])
        if risk:
            new_payload["risk_flags"] = risk if isinstance(risk, list) else []

        if new_payload:
            batch_points.append({
                "id": r.id,
                "payload": new_payload,
            })

        if len(batch_points) >= 100:
            _flush_batch(client, batch_points)
            updated += len(batch_points)
            print(f"[Backfill] Updated {updated}/{len(records_needing_update)}...")
            batch_points = []
            time.sleep(0.1)

    # Flush remaining
    if batch_points:
        _flush_batch(client, batch_points)
        updated += len(batch_points)

    print(f"[Backfill] Done! Updated {updated} records with summary/project_guess/risk_flags")


def _flush_batch(client, batch_points):
    """Set payload for a batch of points."""
    from qdrant_client.models import PointIdsList

    for bp in batch_points:
        try:
            client.set_payload(
                collection_name=QDRANT_COLLECTION,
                payload=bp["payload"],
                points=[bp["id"]],
            )
        except Exception as e:
            print(f"[Backfill] set_payload error for point {bp['id']}: {e}")


if __name__ == "__main__":
    main()
