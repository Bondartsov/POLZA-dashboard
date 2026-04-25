#!/usr/bin/env python3
"""
Backfill script: add cost, total_tokens, summary, project_guess, risk_flags
to existing Qdrant vectors from PostgreSQL.

Run once:
    python3 backfill_enrich_vectors.py
"""
import sys
import os
import json
import time

# Load .env and bootstrap app context
from dotenv import load_dotenv
load_dotenv()

# Import config (triggers init_db + DB engine creation)
import config
from config import gen_summary_get_many, get_session, Generation
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

    # Find records missing ANY enrichable field
    records_needing_update = []
    gen_ids = []

    for r in all_records:
        p = r.payload
        needs_update = (
            "cost" not in p
            or "total_tokens" not in p
            or not p.get("summary")
            or not p.get("project_guess")
        )
        if needs_update:
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

    # Batch fetch generation records for cost/tokens
    print(f"[Backfill] Fetching generation records for cost/tokens...")
    gen_records = {}
    for i in range(0, len(gen_ids), 500):
        batch = gen_ids[i:i + 500]
        try:
            session = get_session()
            try:
                gens = session.query(
                    Generation.id,
                    Generation.cost,
                    Generation.total_tokens,
                ).filter(
                    Generation.id.in_(batch)
                ).all()
                for g in gens:
                    gen_records[g.id] = {
                        "cost": float(g.cost or 0),
                        "total_tokens": int(g.total_tokens or 0),
                    }
            finally:
                session.close()
        except Exception as e:
            print(f"[Backfill] batch gen fetch error: {e}")

    print(f"[Backfill] Got {len(gen_records)} generation records with cost/tokens")

    # Build updates
    updated = 0
    batch_points = []

    for r in records_needing_update:
        gid = r.payload.get("generation_id", "")
        s = summaries.get(gid, {})
        g = gen_records.get(gid, {})

        new_payload = {}

        # From generation_summaries
        if s.get("summary") and not r.payload.get("summary"):
            new_payload["summary"] = s["summary"][:500]
        if s.get("project_guess") and not r.payload.get("project_guess"):
            new_payload["project_guess"] = s["project_guess"]
        risk = s.get("risk_flags", [])
        if risk and not r.payload.get("risk_flags"):
            new_payload["risk_flags"] = risk if isinstance(risk, list) else []

        # From generations table
        if "cost" not in r.payload and g:
            new_payload["cost"] = g.get("cost", 0)
        if "total_tokens" not in r.payload and g:
            new_payload["total_tokens"] = g.get("total_tokens", 0)

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

    print(f"[Backfill] Done! Updated {updated} records with cost/total_tokens/summary/project_guess/risk_flags")


def _flush_batch(client, batch_points):
    """Set payload for a batch of points."""
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
