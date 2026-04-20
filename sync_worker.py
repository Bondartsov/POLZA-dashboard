#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polza.AI Dashboard — Background sync worker.
START_MODULE_CONTRACT
  PURPOSE: Incremental sync of generation records from Polza.AI API to PostgreSQL
  SCOPE: SyncWorker thread (5 min interval), per-key sync, upsert logic
  DEPENDS: db.py (models), requests (HTTP)
  LINKS: M-SYNC
END_MODULE_CONTRACT
"""
import threading
import json as _json
import requests as http_requests
from datetime import datetime, timedelta, timezone
from sqlalchemy.dialects.postgresql import insert as pg_insert
from db import get_session, ApiKey, Generation

POLZA_API = "https://polza.ai/api/v1"
SYNC_INTERVAL = 300  # 5 minutes
SYNC_BUFFER = timedelta(minutes=2)  # overlap to catch boundary records


def _headers(token):
    return {"Authorization": f"Bearer {token}", "Accept": "application/json"}


def _parse_dt(iso_str):
    """Parse ISO datetime string to datetime object."""
    if not iso_str:
        return None
    try:
        return datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except Exception:
        return None


def _upsert_generation(session, item, source_key_name):
    """Insert or update a single generation record."""
    usage = item.get("usage") or {}
    prompt_details = usage.get("prompt_tokens_details") or {}
    completion_details = usage.get("completion_tokens_details") or {}

    stmt = pg_insert(Generation).values(
        id=item["id"],
        model=item.get("model"),
        model_display_name=item.get("modelDisplayName"),
        request_type=item.get("requestType"),
        status=item.get("status"),
        cost=float(item.get("cost") or 0),
        client_cost=float(item.get("clientCost") or 0),
        prompt_tokens=usage.get("prompt_tokens", 0),
        completion_tokens=usage.get("completion_tokens", 0),
        total_tokens=usage.get("total_tokens", 0),
        cached_tokens=prompt_details.get("cached_tokens", 0),
        reasoning_tokens=completion_details.get("reasoning_tokens", 0),
        audio_tokens=prompt_details.get("audio_tokens", 0),
        video_tokens=prompt_details.get("video_tokens", 0),
        generation_time_ms=item.get("generationTimeMs"),
        latency_ms=item.get("latencyMs"),
        created_at_api=_parse_dt(item.get("createdAt")),
        completed_at=_parse_dt(item.get("completedAt")),
        api_key_name=item.get("apiKeyName") or source_key_name,
        api_key_short=item.get("apiKeyShort"),
        api_key_id=item.get("apiKeyId"),
        finish_reason=item.get("finishReason"),
        response_mode=item.get("responseMode"),
        has_log=item.get("hasLog", False),
        final_endpoint_slug=item.get("finalEndpointSlug"),
        api_type=item.get("apiType"),
        provider=item.get("provider"),
        source_key_name=source_key_name,
        usage_data=usage,
    )
    # ON CONFLICT: update mutable fields only
    stmt = stmt.on_conflict_do_update(
        index_elements=["id"],
        set_={
            "status": stmt.excluded.status,
            "cost": stmt.excluded.cost,
            "client_cost": stmt.excluded.client_cost,
            "has_log": stmt.excluded.has_log,
            "completed_at": stmt.excluded.completed_at,
            "finish_reason": stmt.excluded.finish_reason,
        }
    )
    session.execute(stmt)


def _enrich_session_metadata(dbs, gen_id, token):
    """Fetch generation detail and extract session_id/device_id from metadata.externalUserId."""
    try:
        r = http_requests.get(
            f"{POLZA_API}/history/generations/{gen_id}",
            headers=_headers(token), timeout=15
        )
        if r.status_code != 200:
            return False
        detail = r.json()
        ext_user = detail.get("metadata", {}).get("externalUserId")
        if not ext_user:
            return False
        try:
            data = _json.loads(ext_user) if isinstance(ext_user, str) else ext_user
        except (ValueError, TypeError):
            return False
        sid = data.get("session_id")
        did = data.get("device_id")
        if sid or did:
            gen = dbs.query(Generation).get(gen_id)
            if gen:
                if sid:
                    gen.session_id = sid
                if did:
                    gen.device_id = did
                dbs.commit()
            return True
        return False
    except Exception as e:
        print(f"[Sync] Enrich {gen_id[:8]}: {e}")
        return False


def sync_key(session, api_key):
    """Sync a single API key — fetch new records since last_sync_at.
    Returns (new_count, error_string_or_None)."""
    since = api_key.last_sync_at
    if since:
        since = since - SYNC_BUFFER

    page = 1
    total_new = 0
    all_ids = []
    while True:
        params = {"page": page, "limit": 100, "sortBy": "createdAt", "sortOrder": "desc"}
        if since:
            params["dateFrom"] = since.strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            r = http_requests.get(
                f"{POLZA_API}/history/generations",
                headers=_headers(api_key.token),
                params=params, timeout=30
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            api_key.last_error = str(e)[:500]
            session.commit()
            return total_new, str(e)

        items = data.get("items", [])
        if not items:
            break

        for item in items:
            _upsert_generation(session, item, api_key.name)
            all_ids.append(item["id"])
            total_new += 1

        session.commit()
        tp = data.get("meta", {}).get("totalPages", 1)
        if page >= tp:
            break
        page += 1

    # Enrich session metadata for records without it
    if all_ids:
        gens_needing = session.query(Generation).filter(
            Generation.id.in_(all_ids),
            Generation.session_id.is_(None)
        ).all()
        enriched = 0
        for gen in gens_needing:
            if _enrich_session_metadata(session, gen.id, api_key.token):
                enriched += 1
            elif not gen.session_id:
                # Mark as checked (no session data) to avoid re-checking
                gen.session_id = ""
                session.commit()
        if enriched:
            print(f"[Sync] {api_key.name}: enriched {enriched} session metadata")

    # Update key sync metadata
    api_key.last_sync_at = datetime.now(timezone.utc)
    api_key.total_synced = (api_key.total_synced or 0) + total_new
    api_key.last_error = None
    session.commit()
    return total_new, None


def sync_all_keys():
    """Sync all registered API keys. Returns list of per-key results."""
    session = get_session()
    try:
        keys = session.query(ApiKey).all()
        results = []
        for key in keys:
            new_count, error = sync_key(session, key)
            results.append({
                "name": key.name,
                "new": new_count,
                "status": "ok" if not error else "error",
                "error": error,
            })
            print(f"[Sync] {key.name}: +{new_count} new"
                  + (f" ({error})" if error else ""))
        return results
    except Exception as e:
        print(f"[Sync] Fatal: {e}")
        return [{"name": "fatal", "new": 0, "status": "error", "error": str(e)}]
    finally:
        session.close()


class SyncWorker(threading.Thread):
    """Background thread: syncs all keys every SYNC_INTERVAL seconds."""

    def __init__(self):
        super().__init__(daemon=True, name="polza-sync")
        self._stop = threading.Event()
        self._sync_now = threading.Event()
        self.last_results = []
        self.last_time = None

    def run(self):
        # Initial sync on start
        self._do_sync()
        while not self._stop.is_set():
            # Wait for interval OR manual trigger
            self._sync_now.wait(timeout=SYNC_INTERVAL)
            self._sync_now.clear()
            if not self._stop.is_set():
                self._do_sync()

    def _do_sync(self):
        try:
            results = sync_all_keys()
            self.last_results = results
            self.last_time = datetime.now(timezone.utc)
            total_new = sum(r["new"] for r in results if r["new"])
            if total_new:
                print(f"[Sync] Total +{total_new} new records across {len(results)} keys")
        except Exception as e:
            print(f"[Sync] Error: {e}")

    def trigger(self):
        """Trigger immediate sync (non-blocking)."""
        self._sync_now.set()

    def stop(self):
        self._stop.set()
        self._sync_now.set()

    def status(self):
        return {
            "lastSync": self.last_time.isoformat() if self.last_time else None,
            "results": self.last_results,
            "interval": SYNC_INTERVAL,
        }
