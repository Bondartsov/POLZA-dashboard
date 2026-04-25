#!/usr/bin/env python3
"""
Full backfill: rebuild Qdrant payloads with ALL Generation + GenerationSummary fields.

Uses overwrite_payload to replace the entire payload (keeping the vector)
with the complete data from PostgreSQL.

Run once:
    python3 backfill_full_vectors.py
"""
import sys
import os
import time

from dotenv import load_dotenv
load_dotenv()

import config
from config import get_session, Generation
from config import gen_summary_get_many
from embeddings.qdrant import _get_qdrant_client, _qdrant_ensure_collection, QDRANT_COLLECTION
from embeddings.payload import _build_qdrant_payload


def main():
    client = _get_qdrant_client()
    if not client:
        print("[BackfillFull] Qdrant client failed")
        sys.exit(1)

    _qdrant_ensure_collection()
    print(f"[BackfillFull] Connected to Qdrant, collection: {QDRANT_COLLECTION}")

    # Step 1: Scroll ALL records from Qdrant
    print("[BackfillFull] Scrolling all Qdrant records...")
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

    print(f"[BackfillFull] Total records in Qdrant: {len(all_records)}")

    # Step 2: Collect all generation IDs
    gen_ids = []
    qdrant_map = {}  # generation_id -> record (for lookup)
    for r in all_records:
        gid = r.payload.get("generation_id", "")
        if gid:
            gen_ids.append(gid)
            qdrant_map[gid] = r

    # Step 3: Batch-fetch ALL Generation records from PostgreSQL
    print(f"[BackfillFull] Fetching {len(gen_ids)} Generation records from PostgreSQL...")
    gen_records = {}
    for i in range(0, len(gen_ids), 500):
        batch = gen_ids[i:i + 500]
        try:
            session = get_session()
            try:
                gens = session.query(Generation).filter(
                    Generation.id.in_(batch)
                ).all()
                for g in gens:
                    gen_records[g.id] = g.to_dict()
            finally:
                session.close()
        except Exception as e:
            print(f"[BackfillFull] batch gen fetch error: {e}")

    print(f"[BackfillFull] Got {len(gen_records)} Generation records")

    # Step 4: Batch-fetch ALL GenerationSummary records
    print(f"[BackfillFull] Fetching summaries...")
    summaries = gen_summary_get_many(gen_ids)
    print(f"[BackfillFull] Got {len(summaries)} summaries")

    # Step 5: Build full payloads and overwrite
    print("[BackfillFull] Building full payloads and updating Qdrant...")
    updated = 0
    errors = 0
    missing_gen = 0

    for gid, qdrant_rec in qdrant_map.items():
        gen_meta = gen_records.get(gid)
        if not gen_meta:
            missing_gen += 1
            continue

        summary_data = summaries.get(gid, {})
        analysis = {
            "topic": summary_data.get("topic", ""),
            "summary": summary_data.get("summary", ""),
            "is_work": summary_data.get("is_work", True),
            "project_guess": summary_data.get("project_guess", ""),
            "risk_flags": summary_data.get("risk_flags", []),
        }

        # Preserve user_text_snippet from existing payload
        existing_snippet = qdrant_rec.payload.get("user_text_snippet", "")

        new_payload = _build_qdrant_payload(
            gen_meta=gen_meta,
            analysis=analysis,
            user_text_snippet=existing_snippet,
        )

        try:
            client.overwrite_payload(
                collection_name=QDRANT_COLLECTION,
                payload=new_payload,
                points=[qdrant_rec.id],
            )
            updated += 1

            if updated % 200 == 0:
                print(f"[BackfillFull] Updated {updated}/{len(qdrant_map)}...")
                time.sleep(0.05)

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"[BackfillFull] overwrite error for {gid[:16]}: {e}")

    print(f"\n[BackfillFull] DONE!")
    print(f"  Updated: {updated}")
    print(f"  Missing Generation records: {missing_gen}")
    print(f"  Errors: {errors}")
    print(f"  Total fields per record: ~30 (full Generation + summary)")


if __name__ == "__main__":
    main()
