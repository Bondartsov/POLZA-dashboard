#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polza.AI Dashboard v3 — Flask backend with PostgreSQL caching.
Запуск: python polza_dashboard.py [--port 5000] [--debug]

Architecture:
  PostgreSQL (api_keys, generations) <- sync_worker (every 5 min) <- Polza.AI API
  Frontend reads from DB (fast), detail/log proxied through API (needs correct token)
"""
import argparse, json, os, sys, threading, time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_from_directory
import requests as http_requests
from sqlalchemy import func, desc, asc, String as SaString

if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
POLZA_API = "https://polza.ai/api/v1"
SYNC_INTERVAL = 300  # 5 minutes

from db import init_db, get_session, ApiKey, Generation, engine
from db import SessionSummary, summary_get_or_none, summary_upsert
from db import (
    GenerationSummary,
    gen_summary_get_or_none,
    gen_summary_get_many,
    gen_summary_upsert,
    gen_summary_delete,
)
from sync_worker import SyncWorker, sync_all_keys

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="")
sync_worker = None
AUTH_TOKEN = ""

# ─── LLM Provider config (overridden from .env) ────────────────────────────────
# Cloud (Anthropic)
LLM_API_URL = "https://api.anthropic.com/v1/messages"
LLM_MODEL = "claude-haiku-4-5"
LLM_API_KEY = ""

# On-prem (Ollama)
LLM_PROVIDER = "ollama"  # "anthropic" | "ollama" — runtime switchable via /api/provider/set
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_CHAT_MODEL = "qwen3.5:4b"
OLLAMA_EMBED_MODEL = "nomic-embed-text-v2-moe:latest"
OLLAMA_THINKING = False  # if True, allows Qwen thinking mode (slower but deeper)
OLLAMA_TIMEOUT = 120

# Qdrant (vector DB)
QDRANT_URL = "http://localhost:6335"
QDRANT_COLLECTION = "Polza_user_logs"
QDRANT_ENABLED = True  # set False to disable vector storage

# Runtime provider state (in-memory, can be switched via API)
_provider_state = {"provider": "ollama", "auto_analyze": False}  # default, overridden by .env then API


# ─── .env loader ─────────────────────────────────────────────────────────────────

def load_env():
    """Parse .env file — handles inline comments, quoted values, BOM."""
    p = BASE_DIR / ".env"
    if not p.exists():
        return
    raw = p.read_text(encoding="utf-8-sig")
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        if not k or k in os.environ:
            continue
        v = v.strip()
        if v.startswith('"'):
            end = v.find('"', 1)
            v = v[1:end] if end > 0 else v[1:]
        elif v.startswith("'"):
            end = v.find("'", 1)
            v = v[1:end] if end > 0 else v[1:]
        else:
            ci = v.find(" #")
            if ci > 0:
                v = v[:ci]
            v = v.strip().strip("\"'")
        os.environ[k] = v


# ─── Key parsing ──────────────────────────────────────────────────────────────────

def parse_keys_text(text):
    """Parse API keys from Excel paste (Name\\tKey format)."""
    keys = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, key = "", ""
        if "\t" in line:
            parts = line.split("\t")
            idx = next((i for i, p in enumerate(parts)
                        if p.strip().startswith("pza_") or p.strip().startswith("sk-")), -1)
            if idx >= 0:
                key = parts[idx].strip()
                name = " ".join(p.strip() for p in parts[:idx]) or key[-6:]
            else:
                name = parts[0].strip()
                key = parts[-1].strip()
        elif "pza_" in line:
            idx = line.index("pza_")
            name = line[:idx].strip()
            key = line[idx:].strip()
        elif "sk-" in line:
            idx = line.index("sk-")
            name = line[:idx].strip()
            key = line[idx:].strip()
        else:
            continue
        if key and (key.startswith("pza_") or key.startswith("sk-")):
            keys.append({"key": key, "name": name or key[-6:]})
    return keys


# ─── Token resolution (for detail/log proxy) ──────────────────────────────────────

def _resolve_token_for_gen(gen_id):
    """Resolve API token for a generation by looking up source_key_name in DB."""
    session = get_session()
    try:
        gen = session.query(Generation).get(gen_id)
        if gen and gen.source_key_name:
            key = session.query(ApiKey).filter(
                ApiKey.name == gen.source_key_name
            ).first()
            if key:
                return key.token
        return AUTH_TOKEN
    finally:
        session.close()


def _headers(token=None):
    return {"Authorization": f"Bearer {token or AUTH_TOKEN}", "Accept": "application/json"}


# ─── Routes: Static ───────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory(str(STATIC_DIR), "index.html")


# ─── Routes: Config & Health ──────────────────────────────────────────────────────

@app.route("/api/config")
def api_config():
    session = get_session()
    try:
        keys = session.query(ApiKey).all()
        keys_info = [k.to_dict() for k in keys]
        sync_status = sync_worker.status() if sync_worker else {}
        return jsonify({
            "authType": "apikey",
            "tokenPreview": AUTH_TOKEN[:8] + "..." + AUTH_TOKEN[-4:]
            if len(AUTH_TOKEN) > 12 else "***",
            "keys": keys_info,
            "sync": sync_status,
        })
    finally:
        session.close()


@app.route("/api/health")
def api_health():
    session = get_session()
    try:
        total = session.query(Generation).count()
        keys = session.query(ApiKey).count()
        return jsonify({
            "status": "ok", "db": "connected",
            "totalGenerations": total, "totalKeys": keys,
        })
    except Exception as e:
        return jsonify({"status": "error", "detail": str(e)}), 502
    finally:
        session.close()


# ─── Routes: Provider Config ───────────────────────────────────────────────────

@app.route("/api/provider/config")
def api_provider_config():
    """Return current LLM provider settings for frontend toggle."""
    provider = _provider_state["provider"]
    config = {
        "provider": provider,
        "autoAnalyze": _provider_state["auto_analyze"],
        "ollama": {
            "baseUrl": OLLAMA_BASE_URL,
            "chatModel": OLLAMA_CHAT_MODEL,
            "embedModel": OLLAMA_EMBED_MODEL,
            "thinking": OLLAMA_THINKING,
        },
        "anthropic": {
            "model": LLM_MODEL,
            "available": bool(LLM_API_KEY),
        },
    }
    if provider == "ollama":
        config["activeModel"] = OLLAMA_CHAT_MODEL
        config["activeCost"] = "$0.000"
        config["activeEstimate"] = "~5-10 сек" if not OLLAMA_THINKING else "~60 сек"
    else:
        config["activeModel"] = LLM_MODEL
        config["activeCost"] = "~$0.002"
        config["activeEstimate"] = "~2-3 сек"
    return jsonify(config)


@app.route("/api/provider/set", methods=["POST"])
def api_provider_set():
    """Switch LLM provider at runtime. Body: {provider: "ollama"|"anthropic", autoAnalyze?: bool}."""
    data = request.get_json(silent=True) or {}
    provider = data.get("provider", "")
    if provider and provider not in ("ollama", "anthropic"):
        return jsonify({"error": "provider must be 'ollama' or 'anthropic'"}), 400
    if provider:
        _provider_state["provider"] = provider
        print(f"[Provider] switched to {provider}")
    if "autoAnalyze" in data:
        _provider_state["auto_analyze"] = bool(data["autoAnalyze"])
        print(f"[Provider] auto_analyze={_provider_state['auto_analyze']}")
    return jsonify({"ok": True, "provider": _provider_state["provider"], "autoAnalyze": _provider_state["auto_analyze"]})


# ─── Routes: DB-backed generations ────────────────────────────────────────────────

def _apply_filters(query, args):
    """Apply date/type/status/keyName/search filters to a Generation query."""
    date_from = args.get("dateFrom")
    date_to = args.get("dateTo")
    req_type = args.get("requestType")
    status = args.get("status")
    key_name = args.get("keyName")
    search = args.get("search", "").lower()

    if date_from:
        try:
            dt = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
            query = query.filter(Generation.created_at_api >= dt)
        except ValueError:
            pass
    if date_to:
        try:
            dt = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
            query = query.filter(Generation.created_at_api <= dt)
        except ValueError:
            pass
    if req_type:
        query = query.filter(Generation.request_type == req_type)
    if status:
        query = query.filter(Generation.status == status)
    if key_name:
        query = query.filter(
            (Generation.api_key_name == key_name) |
            (Generation.source_key_name == key_name)
        )
    if search:
        query = query.filter(
            (Generation.model_display_name.ilike(f"%{search}%")) |
            (Generation.api_key_name.ilike(f"%{search}%")) |
            (Generation.id.ilike(f"%{search}%"))
        )
    return query


@app.route("/api/db/all")
def api_db_all():
    """Read ALL generations from DB (for charts). No pagination."""
    session = get_session()
    try:
        q = _apply_filters(session.query(Generation), request.args)
        q = q.order_by(desc(Generation.created_at_api))
        items = [g.to_dict() for g in q.all()]
        return jsonify({
            "items": items,
            "meta": {"total": len(items), "totalPages": 1, "page": 1, "limit": len(items)}
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@app.route("/api/db/generations")
def api_db_generations():
    """Read generations from PostgreSQL with pagination, filtering, sorting."""
    session = get_session()
    try:
        q = _apply_filters(session.query(Generation), request.args)
        total = q.count()

        # Sorting
        sort_by = request.args.get("sortBy", "createdAt")
        sort_order = request.args.get("sortOrder", "desc")
        sort_col = {
            "createdAt": Generation.created_at_api,
            "cost": Generation.cost,
            "clientCost": Generation.client_cost,
        }.get(sort_by, Generation.created_at_api)

        q = q.order_by(asc(sort_col) if sort_order == "asc" else desc(sort_col))

        # Pagination
        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 50))
        q = q.offset((page - 1) * limit).limit(limit)

        items = [g.to_dict() for g in q.all()]
        total_pages = max(1, (total + limit - 1) // limit)

        return jsonify({
            "items": items,
            "meta": {"total": total, "totalPages": total_pages, "page": page, "limit": limit}
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


# ─── Routes: API proxy (detail + log) ─────────────────────────────────────────────

@app.route("/api/generations/<gen_id>")
def api_detail(gen_id):
    """Proxy detail request — resolves correct token from DB."""
    token = _resolve_token_for_gen(gen_id)
    try:
        r = http_requests.get(
            f"{POLZA_API}/history/generations/{gen_id}",
            headers=_headers(token), timeout=30
        )
        r.raise_for_status()
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502


@app.route("/api/generations/<gen_id>/log")
def api_log(gen_id):
    """Proxy log request — resolves correct token from DB."""
    token = _resolve_token_for_gen(gen_id)
    try:
        r = http_requests.get(
            f"{POLZA_API}/history/generations/{gen_id}/log",
            headers=_headers(token), timeout=60
        )
        r.raise_for_status()
        return jsonify(r.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ─── Routes: Key management ───────────────────────────────────────────────────────

@app.route("/api/keys", methods=["POST"])
def api_register_keys():
    """Register API keys (from sidebar paste) into PostgreSQL."""
    data = request.get_json(silent=True) or {}
    keys = data.get("keys", [])
    if not keys:
        return jsonify({"error": "No keys provided"}), 400

    session = get_session()
    try:
        registered = []
        for k in keys:
            if not isinstance(k, dict) or not k.get("key"):
                continue
            token = k["key"]
            name = k.get("name", token[-6:])
            key_suffix = token[-6:]

            existing = session.query(ApiKey).filter(ApiKey.token == token).first()
            if existing:
                registered.append(existing.to_dict())
                continue

            api_key = ApiKey(
                name=name, token=token, key_suffix=key_suffix, is_primary=False
            )
            session.add(api_key)
            registered.append(api_key.to_dict())

        session.commit()
        return jsonify({"registered": len(registered), "keys": registered})
    except Exception as e:
        session.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


# ─── Routes: Sync ─────────────────────────────────────────────────────────────────

@app.route("/api/sync", methods=["POST"])
def api_trigger_sync():
    """Trigger async sync (fire and forget)."""
    if sync_worker:
        sync_worker.trigger()
        return jsonify({"status": "triggered"})
    return jsonify({"error": "Sync worker not running"}), 500


@app.route("/api/sync/run", methods=["POST"])
def api_sync_run():
    """Run sync synchronously — waits for all keys, returns results."""
    try:
        results = sync_all_keys()
        total_new = sum(r["new"] for r in results)
        return jsonify({"results": results, "totalNew": total_new})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/sync/status")
def api_sync_status():
    if sync_worker:
        return jsonify(sync_worker.status())
    return jsonify({"status": "not running"})


# ─── Routes: Balance (kept as proxy) ──────────────────────────────────────────────

@app.route("/api/balance")
def api_balance():
    session = get_session()
    try:
        key = session.query(ApiKey).filter(ApiKey.is_primary == True).first()
        token = key.token if key else AUTH_TOKEN
    finally:
        session.close()
    try:
        r = http_requests.get(f"{POLZA_API}/balance", headers=_headers(token), timeout=10)
        if r.status_code == 200:
            return jsonify(r.json())
        return jsonify({"error": f"HTTP {r.status_code}"}), r.status_code
    except Exception as e:
        return jsonify({"error": str(e)}), 502


# ─── Routes: Sessions (Background Backfill) ──────────────────────────────────────

# Global backfill state
_backfill = {
    'running': False,
    'enriched': 0,
    'no_data': 0,
    'errors': 0,
    'remaining': 0,
    'total': 0,
    'error_msg': None,
    'last_update': None,
    'started_at': None,
    'stop_requested': False,
    'thread': None,
    'lock': threading.Lock(),
}


def _backfill_worker():
    """Background thread: enrich session metadata for records where session_id IS NULL."""
    print("[Backfill] Worker started")
    while True:
        with _backfill['lock']:
            if _backfill['stop_requested']:
                _backfill['running'] = False
                print("[Backfill] Stopped by request")
                return

        dbs = get_session()
        try:
            batch = dbs.query(Generation).filter(
                Generation.session_id.is_(None)
            ).order_by(desc(Generation.created_at_api)).limit(50).all()

            if not batch:
                with _backfill['lock']:
                    _backfill['remaining'] = 0
                    _backfill['running'] = False
                    _backfill['last_update'] = datetime.now(timezone.utc).isoformat()
                print("[Backfill] Done — no more records")
                return

            batch_enriched = 0
            batch_no_data = 0
            batch_errors = 0
            for gen in batch:
                with _backfill['lock']:
                    if _backfill['stop_requested']:
                        _backfill['running'] = False
                        print("[Backfill] Stopped mid-batch")
                        return

                token = _resolve_token_for_gen(gen.id)
                try:
                    r = http_requests.get(
                        f"{POLZA_API}/history/generations/{gen.id}",
                        headers=_headers(token), timeout=15
                    )
                    if r.status_code != 200:
                        batch_errors += 1
                        continue
                    detail = r.json()
                    ext_user = detail.get("metadata", {}).get("externalUserId")
                    if ext_user:
                        data = json.loads(ext_user) if isinstance(ext_user, str) else ext_user
                        sid = data.get("session_id")
                        did = data.get("device_id")
                        if sid:
                            gen.session_id = sid
                        if did:
                            gen.device_id = did
                        dbs.commit()
                        if sid or did:
                            batch_enriched += 1
                        else:
                            gen.session_id = ""
                            dbs.commit()
                            batch_no_data += 1
                    else:
                        gen.session_id = ""
                        dbs.commit()
                        batch_no_data += 1
                except Exception as e:
                    batch_errors += 1
                    print(f"[Backfill] Error {gen.id[:16]}: {e}")

            remaining = dbs.query(Generation).filter(
                Generation.session_id.is_(None)
            ).count()

            with _backfill['lock']:
                _backfill['enriched'] += batch_enriched
                _backfill['no_data'] += batch_no_data
                _backfill['errors'] += batch_errors
                _backfill['remaining'] = remaining
                _backfill['last_update'] = datetime.now(timezone.utc).isoformat()

                if batch_errors >= 45:  # too many errors — pause with error
                    _backfill['error_msg'] = f"Слишком много ошибок API ({batch_errors}/50 в последней партии)"
                    _backfill['running'] = False
                    print(f"[Backfill] Paused: {_backfill['error_msg']}")
                    return

            time.sleep(0.5)  # polite delay between batches

        except Exception as e:
            with _backfill['lock']:
                _backfill['error_msg'] = str(e)
                _backfill['running'] = False
            print(f"[Backfill] Fatal error: {e}")
            return
        finally:
            dbs.close()


@app.route("/api/sessions/backfill/start", methods=["POST"])
def api_backfill_start():
    """Start background backfill thread."""
    with _backfill['lock']:
        if _backfill['running']:
            return jsonify({"status": "already_running"})
        # Reset counters on new start
        _backfill['running'] = True
        _backfill['enriched'] = 0
        _backfill['no_data'] = 0
        _backfill['errors'] = 0
        _backfill['error_msg'] = None
        _backfill['stop_requested'] = False
        _backfill['started_at'] = datetime.now(timezone.utc).isoformat()
        _backfill['last_update'] = _backfill['started_at']

        # Count total remaining
        dbs = get_session()
        try:
            _backfill['remaining'] = dbs.query(Generation).filter(
                Generation.session_id.is_(None)
            ).count()
            _backfill['total'] = _backfill['remaining']
        finally:
            dbs.close()

        t = threading.Thread(target=_backfill_worker, daemon=True)
        _backfill['thread'] = t
        t.start()

    return jsonify({"status": "started", "remaining": _backfill['remaining']})


@app.route("/api/sessions/backfill/stop", methods=["POST"])
def api_backfill_stop():
    """Request backfill to stop."""
    with _backfill['lock']:
        _backfill['stop_requested'] = True
    return jsonify({"status": "stopping"})


@app.route("/api/sessions/backfill/retry", methods=["POST"])
def api_backfill_retry():
    """Retry backfill after error (like start but keeps enriched count)."""
    with _backfill['lock']:
        if _backfill['running']:
            return jsonify({"status": "already_running"})
        saved_enriched = _backfill['enriched']
        saved_no_data = _backfill['no_data']
        _backfill['running'] = True
        _backfill['errors'] = 0
        _backfill['error_msg'] = None
        _backfill['stop_requested'] = False
        _backfill['started_at'] = datetime.now(timezone.utc).isoformat()
        _backfill['last_update'] = _backfill['started_at']

        dbs = get_session()
        try:
            _backfill['remaining'] = dbs.query(Generation).filter(
                Generation.session_id.is_(None)
            ).count()
            _backfill['total'] = _backfill['remaining'] + saved_enriched + saved_no_data
        finally:
            dbs.close()

        t = threading.Thread(target=_backfill_worker, daemon=True)
        _backfill['thread'] = t
        t.start()

    return jsonify({"status": "restarted", "remaining": _backfill['remaining']})


@app.route("/api/sessions/backfill/status")
def api_backfill_status():
    """Get current backfill status."""
    with _backfill['lock']:
        return jsonify({
            "running": _backfill['running'],
            "enriched": _backfill['enriched'],
            "noData": _backfill['no_data'],
            "errors": _backfill['errors'],
            "remaining": _backfill['remaining'],
            "total": _backfill['total'],
            "errorMsg": _backfill['error_msg'],
            "lastUpdate": _backfill['last_update'],
            "startedAt": _backfill['started_at'],
        })


@app.route("/api/db/sessions")
def api_db_sessions():
    """Aggregated session data with date filters."""
    dbs = get_session()
    try:
        q = dbs.query(
            Generation.session_id,
            Generation.source_key_name,
            func.min(Generation.created_at_api).label("first_at"),
            func.max(Generation.created_at_api).label("last_at"),
            func.count(Generation.id).label("total_count"),
            func.sum(Generation.cost).label("total_cost"),
            func.sum(Generation.prompt_tokens).label("total_prompt"),
            func.sum(Generation.cached_tokens).label("total_cached"),
            func.sum(Generation.completion_tokens).label("total_completion"),
            func.string_agg(func.distinct(Generation.model_display_name), ",").label("models_str"),
        ).filter(
            Generation.session_id.isnot(None),
            Generation.session_id != "",
        )

        # Apply same date filters as generations
        q = _apply_filters(q, request.args)

        q = q.group_by(Generation.session_id, Generation.source_key_name)
        q = q.order_by(desc("last_at"))

        results = q.all()
        sessions = []
        for r in results:
            cache_pct = round(r.total_cached / r.total_prompt * 100) if (r.total_prompt or 0) > 0 else 0
            sessions.append({
                "sessionId": r.session_id,
                "sourceKey": r.source_key_name,
                "firstAt": r.first_at.isoformat() if r.first_at else None,
                "lastAt": r.last_at.isoformat() if r.last_at else None,
                "totalCount": r.total_count,
                "totalCost": float(r.total_cost or 0),
                "totalPrompt": r.total_prompt or 0,
                "totalCached": r.total_cached or 0,
                "totalCompletion": r.total_completion or 0,
                "cachePct": cache_pct,
                "models": [m for m in (r.models_str or "").split(",") if m],
            })

        return jsonify({"sessions": sessions})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        dbs.close()


# ─── START_BLOCK_ANOMALY
# M-ANOMALY: Rules-based anomaly detection for employee usage patterns

def detect_anomalies(generations_list):
    """
    Analyze employee generation records for anomalies.
    Returns list of {type, severity, count, details} dicts.

    Rules:
    - OFF_HOURS: generation at 22:00-08:00 (severity by count)
    - WEEKEND: generation on Saturday/Sunday
    - UNUSUAL_MODEL: model not in employee's top 3
    - BURST_50_PLUS: >50 generations in any 1-hour window
    """
    anomalies = []
    if not generations_list:
        return anomalies

    # Rule 1: OFF_HOURS (22:00-08:00)
    off_hours = [g for g in generations_list
                 if g.created_at_api and (g.created_at_api.hour >= 22 or g.created_at_api.hour < 8)]
    if off_hours:
        sev = "high" if len(off_hours) > 10 else "medium" if len(off_hours) > 3 else "low"
        anomalies.append({
            "type": "OFF_HOURS", "severity": sev, "count": len(off_hours),
            "details": f"{len(off_hours)} запросов в нерабочее время (22:00–08:00)",
        })

    # Rule 2: WEEKEND
    weekend = [g for g in generations_list
               if g.created_at_api and g.created_at_api.weekday() >= 5]
    if weekend:
        sev = "high" if len(weekend) > 20 else "medium" if len(weekend) > 5 else "low"
        anomalies.append({
            "type": "WEEKEND", "severity": sev, "count": len(weekend),
            "details": f"{len(weekend)} запросов в выходные дни",
        })

    # Rule 3: UNUSUAL_MODEL (not in top 3)
    model_counts = Counter(g.model_display_name for g in generations_list if g.model_display_name)
    if model_counts:
        top3 = set(m for m, _ in model_counts.most_common(3))
        unusual = [g for g in generations_list
                   if g.model_display_name and g.model_display_name not in top3]
        if unusual:
            unusual_models = sorted(set(g.model_display_name for g in unusual))
            anomalies.append({
                "type": "UNUSUAL_MODEL", "severity": "low", "count": len(unusual),
                "details": f"Необычные модели: {', '.join(unusual_models)}",
            })

    # Rule 4: BURST_50_PLUS (>50 gens in 1 hour)
    if len(generations_list) >= 50:
        sorted_gens = sorted(
            [g for g in generations_list if g.created_at_api],
            key=lambda g: g.created_at_api,
        )
        for i in range(len(sorted_gens)):
            t0 = sorted_gens[i].created_at_api
            count = sum(
                1 for j in range(max(0, i - 1), len(sorted_gens))
                if abs((sorted_gens[j].created_at_api - t0).total_seconds()) <= 3600
            )
            if count > 50:
                anomalies.append({
                    "type": "BURST_50_PLUS", "severity": "high", "count": count,
                    "details": f"Всплеск: {count} запросов за 1 час ({t0.strftime('%d.%m %H:%M')})",
                })
                break

    return anomalies

# ─── END_BLOCK_ANOMALY


# ─── START_BLOCK_EMPLOYEE_REPORT
# M-EMPLOYEE-REPORT: Aggregated employee report API (no AI, pure SQL)

@app.route("/api/employee-report/list")
def api_employee_report_list():
    """Summary table: each employee with cost/tokens/requests for period."""
    dbs = get_session()
    try:
        q = dbs.query(
            Generation.source_key_name,
            func.count(Generation.id).label("total_requests"),
            func.sum(Generation.cost).label("total_cost"),
            func.sum(Generation.total_tokens).label("total_tokens"),
            func.sum(Generation.cached_tokens).label("total_cached"),
            func.count(func.distinct(Generation.session_id)).label("session_count"),
        ).filter(
            Generation.source_key_name.isnot(None),
            Generation.source_key_name != "",
        )

        date_from = request.args.get("dateFrom")
        date_to = request.args.get("dateTo")
        if date_from:
            try:
                dt = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
                q = q.filter(Generation.created_at_api >= dt)
            except ValueError:
                pass
        if date_to:
            try:
                dt = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
                q = q.filter(Generation.created_at_api <= dt)
            except ValueError:
                pass

        q = q.group_by(Generation.source_key_name).order_by(desc("total_cost"))
        results = q.all()

        employees = []
        for r in results:
            employees.append({
                "name": r.source_key_name,
                "totalRequests": r.total_requests,
                "totalCost": float(r.total_cost or 0),
                "totalTokens": r.total_tokens or 0,
                "totalCached": r.total_cached or 0,
                "sessionCount": r.session_count,
            })

        return jsonify({"employees": employees, "total": len(employees)})
    except Exception as e:
        print(f"[EmployeeReport][report_list] ERROR: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        dbs.close()


@app.route("/api/employee-report")
def api_employee_report():
    """Aggregated report for one employee: cost, sessions, anomalies, heatmap."""
    employee = request.args.get("employee", "")
    date_from_str = request.args.get("dateFrom", "")
    date_to_str = request.args.get("dateTo", "")
    period = request.args.get("period", "")

    now = datetime.now(timezone.utc)
    if period == "today":
        date_from_str = now.strftime("%Y-%m-%d")
    elif period == "7d":
        date_from_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    elif period == "30d":
        date_from_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")

    dbs = get_session()
    try:
        q = dbs.query(Generation).filter(Generation.source_key_name == employee)

        if date_from_str:
            try:
                dt = datetime.fromisoformat(date_from_str.replace("Z", "+00:00"))
                q = q.filter(Generation.created_at_api >= dt)
            except ValueError:
                pass
        if date_to_str:
            try:
                dt = datetime.fromisoformat(date_to_str.replace("Z", "+00:00"))
                q = q.filter(Generation.created_at_api <= dt)
            except ValueError:
                pass

        generations = q.order_by(desc(Generation.created_at_api)).all()

        # Totals
        total_cost = sum(g.cost or 0 for g in generations)
        total_tokens = sum(g.total_tokens or 0 for g in generations)
        total_cached = sum(g.cached_tokens or 0 for g in generations)
        total_completion = sum(g.completion_tokens or 0 for g in generations)

        # Build sessions map
        sessions_map = {}
        models_counter = Counter()
        for g in generations:
            sid = g.session_id or ""
            if sid not in sessions_map:
                sessions_map[sid] = {
                    "sessionId": sid,
                    "sourceKey": g.source_key_name,
                    "firstAt": None, "lastAt": None,
                    "totalCount": 0, "totalCost": 0,
                    "models": set(),
                }
            s = sessions_map[sid]
            s["totalCount"] += 1
            s["totalCost"] += g.cost or 0
            if g.created_at_api:
                if not s["firstAt"] or g.created_at_api < s["firstAt"]:
                    s["firstAt"] = g.created_at_api
                if not s["lastAt"] or g.created_at_api > s["lastAt"]:
                    s["lastAt"] = g.created_at_api
            if g.model_display_name:
                s["models"].add(g.model_display_name)
                models_counter[g.model_display_name] += 1

        # Convert sets → lists, serialize dates
        sessions = []
        for sid, s in sessions_map.items():
            s["models"] = sorted(s["models"])
            s["firstAt"] = s["firstAt"].isoformat() if s["firstAt"] else None
            s["lastAt"] = s["lastAt"].isoformat() if s["lastAt"] else None
            s["totalCost"] = float(s["totalCost"])
            # Attach cached summary if exists
            if sid:
                cached_summary = summary_get_or_none(sid)
                if cached_summary:
                    s["_summary"] = cached_summary.to_dict()
            sessions.append(s)
        sessions.sort(key=lambda x: x["lastAt"] or "", reverse=True)

        # Anomaly detection
        anomalies = detect_anomalies(generations)

        # Heatmap: [weekday][hour] → count
        heatmap = [[0] * 24 for _ in range(7)]
        for g in generations:
            if g.created_at_api:
                heatmap[g.created_at_api.weekday()][g.created_at_api.hour] += 1

        # Models breakdown
        models = [{"name": m, "count": c} for m, c in models_counter.most_common(10)]

        print(f"[EmployeeReport][report] returned {len(generations)} gens, {len(sessions)} sessions, {len(anomalies)} anomalies for {employee}")
        return jsonify({
            "employee": employee,
            "totals": {
                "cost": float(total_cost),
                "tokens": total_tokens,
                "cached": total_cached,
                "completion": total_completion,
                "requests": len(generations),
                "sessions": len([s for s in sessions if s["sessionId"]]),
            },
            "sessions": sessions,
            "anomalies": anomalies,
            "heatmap": heatmap,
            "models": models,
        })
    except Exception as e:
        print(f"[EmployeeReport][report] ERROR: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        dbs.close()

# ─── END_BLOCK_EMPLOYEE_REPORT


# ─── START_BLOCK_SESSION_SUMMARIZER
# M-SESSION-SUMMARIZER: LLM-powered session summarization + background thread

# Summarize-all state (same pattern as backfill)
_summarize = {
    'running': False,
    'done': 0,
    'total': 0,
    'errors': 0,
    'error_msg': None,
    'last_update': None,
    'started_at': None,
    'stop_requested': False,
    'thread': None,
    'lock': threading.Lock(),
    'employee': None,
}

SUMMARIZE_SYSTEM_PROMPT = """Ты — аналитик корпоративного AI-мониторинга. Проанализируй промпты пользователя из сессии чата и верни СТРОГО JSON:

{
  "summary": "Краткое описание задач (1-3 предложения, русский)",
  "topic": "Основная тема (2-4 слова, русский)",
  "is_work": true,
  "project_guess": "Предположение о проекте (если понятно)",
  "risk_flags": []
}

Правила:
- is_work = true если задачи выглядят рабочими (код, аналитика, документация, тестирование)
- is_work = false если похоже на личное использование (игры, рецепты, личные письма)
- risk_flags: массив строк — "off_hours", "personal", "unusual_model", "high_cost"
- Отвечай ТОЛЬКО JSON, без markdown, без пояснений"""


def _summarize_single_session(session_id: str):
    """
    Summarize a single session: check cache → fetch logs → call LLM → cache result.
    Returns dict with summary data or raises Exception.
    """
    # START_BLOCK_SUMMARY_CACHE_CHECK
    cached = summary_get_or_none(session_id)
    if cached:
        print(f"[Summarizer][cache_hit] session_id={session_id[:16]}")
        return cached.to_dict()
    # END_BLOCK_SUMMARY_CACHE_CHECK

    # START_BLOCK_FETCH_LOGS
    dbs = get_session()
    try:
        gens = dbs.query(Generation).filter(
            Generation.session_id == session_id
        ).order_by(desc(Generation.created_at_api)).all()

        if not gens:
            raise ValueError("NO_GENERATIONS")

        source_key = gens[0].source_key_name or ""

        # Resolve token for this session's key
        key_row = dbs.query(ApiKey).filter(
            ApiKey.name == source_key
        ).first()
        token = key_row.token if key_row else AUTH_TOKEN

        # Fetch user messages from generation logs
        user_messages = []
        for gen in gens[:20]:  # limit to 20 generations
            try:
                r = http_requests.get(
                    f"{POLZA_API}/history/generations/{gen.id}/log",
                    headers=_headers(token), timeout=30
                )
                if r.status_code != 200:
                    continue
                log_data = r.json()
                msgs = log_data.get("request", {}).get("messages", [])
                for m in msgs:
                    if m.get("role") == "user" and m.get("content"):
                        content = m["content"]
                        if isinstance(content, str) and len(content) > 10:
                            user_messages.append(content[:500])
            except Exception as e:
                print(f"[Summarizer][fetch_log] skip {gen.id[:16]}: {e}")
                continue
    finally:
        dbs.close()
    # END_BLOCK_FETCH_LOGS

    if not user_messages:
        raise ValueError("NO_USER_MESSAGES")

    # Truncate total text to ~3000 chars for LLM
    total_text = "\n---\n".join(user_messages)[:3000]

    # START_BLOCK_CALL_LLM
    llm_payload = {
        "model": LLM_MODEL,
        "max_tokens": 700,
        "temperature": 0.3,
        # cache_control on system prompt → Anthropic reuses cached prefix
        "system": [
            {
                "type": "text",
                "text": SUMMARIZE_SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            },
        ],
        "messages": [
            {"role": "user", "content": f"Промпты из сессии (показаны первые 500 символов каждого):\n\n{total_text}"},
        ],
    }

    _llm_headers = {
        "x-api-key": LLM_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    r = http_requests.post(
        LLM_API_URL,
        headers=_llm_headers,
        json=llm_payload,
        timeout=60,
    )

    print(f"[Summarizer][LLM] Status: {r.status_code}")

    if r.status_code != 200:
        print(f"[Summarizer][LLM] Body: {r.text[:300]}")
        raise ValueError(f"LLM_API_ERROR: HTTP {r.status_code} - {r.text[:100]}")

    try:
        llm_response = r.json()
    except Exception as e:
        print(f"[Summarizer][LLM] JSON parse error: {e}")
        raise ValueError("LLM returned non-JSON response")

    llm_cost = 0.0
    try:
        usage = llm_response.get("usage", {})
        llm_cost = (usage.get("input_tokens", 0) * 0.0008 + usage.get("output_tokens", 0) * 0.004) / 1000
    except:
        pass

    content = ""
    for block in llm_response.get("content", []):
        if block.get("type") == "text":
            content += block.get("text", "")
    if not content:
        raise ValueError("LLM returned empty content")

    print(f"[Summarizer][summarize_session][BLOCK_CALL_LLM] LLM response received ({len(content)} chars)")
    # END_BLOCK_CALL_LLM

    # START_BLOCK_PARSE_RESPONSE
    # Extract JSON from response (handle markdown wrapping)
    json_str = content.strip()
    if json_str.startswith("```"):
        lines = json_str.split("\n")
        json_str = "\n".join(lines[1:-1])
    if json_str.startswith("json"):
        json_str = json_str[4:].strip()

    parsed = json.loads(json_str)
    print(f"[Summarizer][summarize_session][BLOCK_PARSE_RESPONSE] parsed OK: topic={parsed.get('topic', '?')}")
    # END_BLOCK_PARSE_RESPONSE

    # Cache result
    summary_upsert(
        session_id=session_id,
        source_key=source_key,
        summary=parsed.get("summary", ""),
        topic=parsed.get("topic", ""),
        is_work=parsed.get("is_work", True),
        project_guess=parsed.get("project_guess"),
        risk_flags=parsed.get("risk_flags", []),
        prompt_hashes=None,
        llm_cost=llm_cost,
    )

    result = {
        "sessionId": session_id,
        "sourceKey": source_key,
        "summary": parsed.get("summary", ""),
        "topic": parsed.get("topic", ""),
        "isWork": parsed.get("is_work", True),
        "projectGuess": parsed.get("project_guess"),
        "riskFlags": parsed.get("risk_flags", []),
        "llmCost": llm_cost,
        "cached": False,
    }
    return result


@app.route("/api/session/summarize", methods=["POST", "GET"])
def api_session_summarize():
    """Summarize a single session — returns from cache or calls LLM."""
    session_id = request.args.get("sessionId") or (request.json or {}).get("sessionId", "")
    if not session_id:
        return jsonify({"error": "sessionId required"}), 400

    try:
        result = _summarize_single_session(session_id)
        return jsonify(result)
    except ValueError as e:
        err = str(e)
        print(f"[Summarizer][summarize] ERROR: {err} for session {session_id[:16]}")
        if err == "NO_GENERATIONS":
            return jsonify({"error": "У сессии нет генераций в БД", "code": "NO_GENERATIONS"}), 404
        if err == "NO_USER_MESSAGES":
            return jsonify({"error": "Не удалось получить промпты из логов", "code": "NO_USER_MESSAGES"}), 404
        return jsonify({"error": err, "sessionId": session_id}), 500
    except Exception as e:
        print(f"[Summarizer][summarize] CRITICAL ERROR: {e} for session {session_id[:16]}")
        import traceback
        traceback.print_exc()
        return jsonify({"error": "Internal server error during summarization", "details": str(e), "sessionId": session_id}), 500


# ─── Generation-level summarize (per single log) ──────────────────────────────────
# START_BLOCK_GEN_SUMMARIZE_PROMPT
# Extended prompt — asks for 3-5 sentence detailed summary instead of 1-2.

GEN_SUMMARIZE_PROMPT = """Ты — аналитик корпоративного AI-мониторинга. Твоя задача — подробно описать,
что делал сотрудник в этом запросе к AI-модели, и оценить его с точки зрения пользы для работы.

Верни СТРОГО JSON в таком формате (без markdown, без комментариев, только JSON):

{
  "summary": "Подробное описание (3-5 предложений, русский). Укажи: (1) что сотрудник делает — конкретное действие/задача; (2) контекст или проект, если ясен; (3) используемые технологии/инструменты; (4) цель или ожидаемый результат. Избегай общих фраз — только конкретика из промпта.",
  "topic": "Основная тема (2-5 слов, русский, например: 'Рефакторинг Python API', 'Отладка SQL', 'Документация React')",
  "is_work": true,
  "project_guess": "Название/описание проекта 1 фразой, если угадывается из кода или контекста. Иначе null.",
  "risk_flags": []
}

Правила оценки:
- is_work = true если запрос рабочий: код, документация, аналитика, тестирование, DevOps, обучение по теме работы.
- is_work = false если личное: игры, развлечения, рецепты, гадания, личная переписка, домашние задания не по работе.
- risk_flags — массив из перечисленных строк (включай только применимые):
    * "personal"       — явно личное использование
    * "off_hours"      — вне рабочего времени (не используй, время не передано)
    * "unusual_model"  — запрос к странной/дорогой модели для простой задачи
    * "high_cost"      — запрос явно дорогой/неоптимальный
    * "sensitive"      — содержит чувствительные данные (пароли, персональные данные, секреты)

Отвечай ТОЛЬКО JSON. Никаких пояснений до или после."""
# END_BLOCK_GEN_SUMMARIZE_PROMPT


# START_BLOCK_GEN_SUMMARIZE_HELPERS

def _parse_llm_json(raw_text: str) -> dict:
    """Robust JSON parser for LLM output. Handles markdown wrapping, extra text."""
    json_str = raw_text.strip()
    # Try direct parse
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        pass
    # Extract from ```json ... ``` code block
    import re
    m = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", json_str, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except json.JSONDecodeError:
            pass
    # Extract first { ... } block
    m = re.search(r"\{[^{}]*\}", json_str, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    # Last resort: extract nested { ... } with depth tracking
    depth = 0
    start = None
    for i, ch in enumerate(json_str):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start is not None:
                try:
                    return json.loads(json_str[start:i + 1])
                except json.JSONDecodeError:
                    start = None
    raise json.JSONDecodeError("Could not extract JSON from LLM output", json_str, 0)


def _llm_call_anthropic(user_text: str):
    """Call Anthropic Claude API. Returns (parsed_dict, usage_info)."""
    llm_payload = {
        "model": LLM_MODEL,
        "max_tokens": 600,
        "temperature": 0.2,
        "system": [
            {
                "type": "text",
                "text": GEN_SUMMARIZE_PROMPT,
                "cache_control": {"type": "ephemeral"},
            },
        ],
        "messages": [
            {"role": "user", "content": f"Запрос пользователя к AI-модели:\n\n{user_text}"},
        ],
    }
    _llm_headers = {
        "x-api-key": LLM_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    llm_r = http_requests.post(
        LLM_API_URL, headers=_llm_headers, json=llm_payload, timeout=45,
    )
    if llm_r.status_code != 200:
        raise ValueError(f"Anthropic HTTP {llm_r.status_code}: {llm_r.text[:300]}")

    llm_response = llm_r.json()
    llm_content = ""
    for block in llm_response.get("content", []):
        if block.get("type") == "text":
            llm_content += block.get("text", "")

    if not llm_content:
        raise ValueError("Anthropic returned empty content")

    parsed = _parse_llm_json(llm_content)

    usage = llm_response.get("usage", {}) or {}
    usage_info = {
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "cache_creation_input_tokens": usage.get("cache_creation_input_tokens", 0),
        "cache_read_input_tokens": usage.get("cache_read_input_tokens", 0),
    }
    cost = (
        usage_info["input_tokens"] * 1.0
        + usage_info["output_tokens"] * 5.0
        + usage_info["cache_creation_input_tokens"] * 1.25
        + usage_info["cache_read_input_tokens"] * 0.10
    ) / 1_000_000
    usage_info["cost_usd"] = round(cost, 6)
    usage_info["model"] = LLM_MODEL
    usage_info["provider"] = "anthropic"
    return parsed, usage_info


def _llm_call_ollama(user_text: str):
    """Call Ollama (Qwen) via native /api/chat endpoint. Returns (parsed_dict, usage_info)."""
    system_prompt = GEN_SUMMARIZE_PROMPT

    payload = {
        "model": OLLAMA_CHAT_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Запрос пользователя к AI-модели:\n\n{user_text}"},
        ],
        "stream": False,
        "think": OLLAMA_THINKING,  # False = fast (~2-5s), True = deep thinking (~60s)
    }
    # Use native /api/chat for think parameter support (OpenAI-compatible lacks this)
    chat_url = f"{OLLAMA_BASE_URL}/api/chat"

    r = http_requests.post(chat_url, json=payload, timeout=OLLAMA_TIMEOUT)
    if r.status_code != 200:
        raise ValueError(f"Ollama HTTP {r.status_code}: {r.text[:500]}")

    data = r.json()
    content = ""
    msg = data.get("message", {})
    if msg:
        content = msg.get("content", "")

    if not content:
        raise ValueError("Ollama returned empty content")

    parsed = _parse_llm_json(content)

    # Ollama native API usage (prompt_eval_count / eval_count)
    usage_info = {
        "input_tokens": data.get("prompt_eval_count", 0) or 0,
        "output_tokens": data.get("eval_count", 0) or 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cost_usd": 0.0,
        "model": OLLAMA_CHAT_MODEL,
        "provider": "ollama",
    }
    return parsed, usage_info


def _llm_call_summarize(user_text: str):
    """
    Dispatcher: call current provider (Anthropic or Ollama).
    Returns tuple (parsed_dict, usage_dict).
    """
    provider = _provider_state["provider"]
    if provider == "ollama":
        return _llm_call_ollama(user_text)
    else:
        return _llm_call_anthropic(user_text)


# ─── Embedding pipeline ────────────────────────────────────────────────────────

_qdrant_client = None

def _get_qdrant_client():
    """Lazy-init Qdrant client. Returns None if qdrant_client not installed or disabled."""
    global _qdrant_client
    if not QDRANT_ENABLED:
        return None
    if _qdrant_client is not None:
        return _qdrant_client
    try:
        from qdrant_client import QdrantClient
        _qdrant_client = QdrantClient(url=QDRANT_URL, timeout=10)
        print(f"[Qdrant] connected to {QDRANT_URL}")
        return _qdrant_client
    except ImportError:
        print("[Qdrant] qdrant_client not installed — embeddings disabled")
        return None
    except Exception as e:
        print(f"[Qdrant] connection failed: {e}")
        return None


def _qdrant_ensure_collection():
    """Create Polza_user_logs collection if it doesn't exist."""
    client = _get_qdrant_client()
    if not client:
        return False
    try:
        from qdrant_client.models import Distance, VectorParams
        collections = client.get_collections().collections
        names = [c.name for c in collections]
        if QDRANT_COLLECTION not in names:
            client.create_collection(
                collection_name=QDRANT_COLLECTION,
                vectors_config=VectorParams(size=768, distance=Distance.COSINE),
            )
            print(f"[Qdrant] created collection '{QDRANT_COLLECTION}' (768-dim cosine)")
        return True
    except Exception as e:
        print(f"[Qdrant] ensure_collection error: {e}")
        return False


def _embed_text(text: str):
    """Get embedding vector from Ollama (nomic-embed-text-v2-moe). Returns list[float] or None."""
    if not text or not text.strip():
        return None
    try:
        r = http_requests.post(
            f"{OLLAMA_BASE_URL}/api/embed",
            json={"model": OLLAMA_EMBED_MODEL, "input": text[:2000]},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"[Embed] Ollama HTTP {r.status_code}: {r.text[:200]}")
            return None
        data = r.json()
        embeddings = data.get("embeddings", [])
        if embeddings and len(embeddings[0]) == 768:
            return embeddings[0]
        print(f"[Embed] unexpected response shape: {len(embeddings)} vectors")
        return None
    except Exception as e:
        print(f"[Embed] error: {e}")
        return None


def _qdrant_upsert(gen_id: str, vector: list, payload: dict):
    """Store embedding in Qdrant. Returns True on success."""
    client = _get_qdrant_client()
    if not client or not vector:
        return False
    try:
        from qdrant_client.models import PointStruct
        import hashlib
        # Use UUID-based integer for point ID (Qdrant requires int or UUID)
        point_id = int(hashlib.md5(gen_id.encode()).hexdigest()[:16], 16)
        client.upsert(
            collection_name=QDRANT_COLLECTION,
            points=[
                PointStruct(id=point_id, vector=vector, payload=payload)
            ],
        )
        print(f"[Qdrant] upsert gen_id={gen_id[:16]} dim={len(vector)}")
        return True
    except Exception as e:
        print(f"[Qdrant] upsert error: {e}")
        return False


def _extract_user_text_from_log(log_data: dict, limit_chars: int = 4000) -> str:
    """Extract concatenated user messages from a Polza generation log."""
    msgs = log_data.get("request", {}).get("messages", [])
    user_parts = []
    for m in msgs:
        role = m.get("role", "")
        content = m.get("content", "")
        if role == "user" and content:
            user_parts.append(str(content)[:1200])

    total_text = "\n---\n".join(user_parts)[:limit_chars]

    # Fallback: if no user role messages, take any non-empty content
    if not total_text.strip():
        for m in msgs:
            content = m.get("content", "")
            if content:
                user_parts.append(str(content)[:1200])
        total_text = "\n---\n".join(user_parts)[:limit_chars]

    return total_text.strip()

# END_BLOCK_GEN_SUMMARIZE_HELPERS


@app.route("/api/generation/summarize", methods=["POST"])
def api_generation_summarize():
    """
    Summarize a single generation by ID.
    Flow: check DB cache → if miss, fetch log from Polza, call LLM, store in DB → return.
    Optional query/body: ?force=1 to bypass cache and re-summarize.
    """
    data = request.get_json(silent=True) or {}
    gen_id = data.get("generationId", "")
    force = bool(data.get("force") or request.args.get("force"))
    if not gen_id:
        return jsonify({"error": "generationId required"}), 400

    # START_BLOCK_GEN_CACHE_CHECK
    if not force:
        cached = gen_summary_get_or_none(gen_id)
        if cached:
            print(f"[GenSummarize][cache_hit] generation_id={gen_id[:16]}")
            return jsonify(cached.to_dict())
    # END_BLOCK_GEN_CACHE_CHECK

    try:
        token = _resolve_token_for_gen(gen_id)

        # Fetch generation log from Polza
        r = http_requests.get(
            f"{POLZA_API}/history/generations/{gen_id}/log",
            headers=_headers(token), timeout=30,
        )
        if r.status_code != 200:
            return jsonify({
                "topic": "Лог недоступен",
                "summary": f"Не удалось получить лог генерации (HTTP {r.status_code}).",
                "isWork": True, "generationId": gen_id, "cached": False,
            }), 200

        total_text = _extract_user_text_from_log(r.json(), limit_chars=4000)

        if not total_text:
            return jsonify({
                "topic": "Пустой запрос",
                "summary": "В логе нет текста промпта для анализа.",
                "isWork": True, "generationId": gen_id, "cached": False,
            }), 200

        # START_BLOCK_GEN_CALL_LLM
        provider = _provider_state["provider"]
        active_model = OLLAMA_CHAT_MODEL if provider == "ollama" else LLM_MODEL
        print(f"[GenSummarize][LLM] provider={provider} model={active_model} text_chars={len(total_text)}")

        # Parallel: LLM summarize + embedding
        llm_result = [None, None]  # [parsed, usage]
        embed_result = [None]  # [vector]

        def _run_llm():
            llm_result[0], llm_result[1] = _llm_call_summarize(total_text)

        def _run_embed():
            embed_result[0] = _embed_text(total_text)

        with ThreadPoolExecutor(max_workers=2) as pool:
            llm_future = pool.submit(_run_llm)
            embed_future = pool.submit(_run_embed)
            # Wait for both (LLM is the long pole)
            llm_future.result(timeout=OLLAMA_TIMEOUT if provider == "ollama" else 45)
            embed_future.result(timeout=30)

        parsed, usage = llm_result
        print(
            f"[GenSummarize][LLM] ok provider={provider} "
            f"input={usage['input_tokens']} output={usage['output_tokens']} "
            f"cost=${usage['cost_usd']:.6f}"
        )
        # END_BLOCK_GEN_CALL_LLM

        # START_BLOCK_GEN_CACHE_STORE
        try:
            gen_summary_upsert(
                generation_id=gen_id,
                summary=parsed.get("summary", ""),
                topic=parsed.get("topic", ""),
                is_work=parsed.get("is_work", True),
                project_guess=parsed.get("project_guess"),
                risk_flags=parsed.get("risk_flags", []),
                llm_model=usage.get("model", active_model),
                llm_cost=usage["cost_usd"],
                cache_creation_tokens=usage.get("cache_creation_input_tokens", 0),
                cache_read_tokens=usage.get("cache_read_input_tokens", 0),
                input_tokens=usage["input_tokens"],
                output_tokens=usage["output_tokens"],
            )
        except Exception as e:
            print(f"[GenSummarize][cache_store] non-fatal: {e}")
        # END_BLOCK_GEN_CACHE_STORE

        # START_BLOCK_GEN_VECTOR_STORE
        vector_stored = False
        if embed_result[0]:
            # Fetch generation metadata for payload
            dbs = get_session()
            gen_meta = None
            try:
                gen_obj = dbs.query(Generation).get(gen_id)
                if gen_obj:
                    gen_meta = gen_obj.to_dict()
            finally:
                dbs.close()

            qdrant_payload = {
                "generation_id": gen_id,
                "user_text_snippet": total_text[:200],
                "topic": parsed.get("topic", ""),
                "is_work": parsed.get("is_work", True),
                "session_id": gen_meta.get("session_id", "") if gen_meta else "",
                "api_key_name": gen_meta.get("sourceKeyName", "") if gen_meta else "",
                "model_used": gen_meta.get("modelDisplayName", "") if gen_meta else "",
                "created_at": gen_meta.get("createdAt", "") if gen_meta else "",
            }
            vector_stored = _qdrant_upsert(gen_id, embed_result[0], qdrant_payload)
        # END_BLOCK_GEN_VECTOR_STORE

        return jsonify({
            "generationId": gen_id,
            "summary": parsed.get("summary", ""),
            "topic": parsed.get("topic", ""),
            "isWork": parsed.get("is_work", True),
            "projectGuess": parsed.get("project_guess"),
            "riskFlags": parsed.get("risk_flags", []),
            "llmModel": usage.get("model", active_model),
            "llmCost": usage["cost_usd"],
            "cacheCreationTokens": usage.get("cache_creation_input_tokens", 0),
            "cacheReadTokens": usage.get("cache_read_input_tokens", 0),
            "inputTokens": usage["input_tokens"],
            "outputTokens": usage["output_tokens"],
            "provider": provider,
            "vectorStored": vector_stored,
            "cached": False,
        })

    except json.JSONDecodeError as e:
        print(f"[GenSummarize] JSON decode error: {e}")
        return jsonify({
            "topic": "Ошибка разбора ответа LLM",
            "summary": "Модель вернула невалидный JSON — попробуйте ещё раз.",
            "isWork": True, "generationId": gen_id, "cached": False,
        }), 200
    except Exception as e:
        print(f"[GenSummarize] ERROR: {e}")
        return jsonify({
            "error": str(e),
            "topic": "Ошибка",
            "summary": f"{e}",
            "generationId": gen_id,
            "cached": False,
        }), 500


@app.route("/api/generation-summaries", methods=["GET", "POST"])
def api_generation_summaries_batch():
    """
    Batch-fetch cached summaries for a list of generation IDs.
    Used by frontend to pre-populate the 🧠 badges on table render.
    Input: {ids: [...]} (POST JSON) or ?ids=id1,id2,... (GET).
    Output: {summaries: {id: {...}, ...}}
    """
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        ids = data.get("ids") or []
    else:
        raw = request.args.get("ids", "")
        ids = [x.strip() for x in raw.split(",") if x.strip()]

    if not ids:
        return jsonify({"summaries": {}})

    # Cap to avoid huge queries
    ids = ids[:500]
    try:
        found = gen_summary_get_many(ids)
        return jsonify({"summaries": found})
    except Exception as e:
        print(f"[GenSummaries][batch] ERROR: {e}")
        return jsonify({"summaries": {}, "error": str(e)}), 500


@app.route("/api/generation/summary", methods=["DELETE"])
def api_generation_summary_delete():
    """Delete cached summary for a generation — forces regeneration next call."""
    gen_id = request.args.get("generationId") or (request.get_json(silent=True) or {}).get("generationId", "")
    if not gen_id:
        return jsonify({"error": "generationId required"}), 400
    try:
        gen_summary_delete(gen_id)
        return jsonify({"ok": True, "generationId": gen_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── Analyze ALL generations (background) ─────────────────────────────────────

_analyze_all = {
    "running": False,
    "paused": False,
    "total": 0,
    "done": 0,
    "errors": 0,
    "skipped": 0,
    "started_at": None,
    "last_update": None,
    "stop_requested": False,
    "thread": None,
    "lock": threading.Lock(),
}


def _analyze_single_gen(gen_id: str) -> dict:
    """Analyze one generation: fetch log → LLM + embed → store. Returns status."""
    # Check cache first
    cached = gen_summary_get_or_none(gen_id)
    if cached:
        return {"status": "skipped"}

    try:
        token = _resolve_token_for_gen(gen_id)
        r = http_requests.get(
            f"{POLZA_API}/history/generations/{gen_id}/log",
            headers=_headers(token), timeout=30,
        )
        if r.status_code != 200:
            return {"status": "error", "detail": f"HTTP {r.status_code}"}

        total_text = _extract_user_text_from_log(r.json(), limit_chars=4000)
        if not total_text:
            return {"status": "skipped", "detail": "empty text"}

        # Parallel: LLM + embed
        llm_result = [None, None]
        embed_result = [None]

        def _run_llm():
            llm_result[0], llm_result[1] = _llm_call_summarize(total_text)

        def _run_embed():
            embed_result[0] = _embed_text(total_text)

        with ThreadPoolExecutor(max_workers=2) as pool:
            llm_future = pool.submit(_run_llm)
            embed_future = pool.submit(_run_embed)
            llm_future.result(timeout=OLLAMA_TIMEOUT if _provider_state["provider"] == "ollama" else 45)
            embed_future.result(timeout=30)

        parsed, usage = llm_result

        # Store in PostgreSQL
        try:
            gen_summary_upsert(
                generation_id=gen_id,
                summary=parsed.get("summary", ""),
                topic=parsed.get("topic", ""),
                is_work=parsed.get("is_work", True),
                project_guess=parsed.get("project_guess"),
                risk_flags=parsed.get("risk_flags", []),
                llm_model=usage.get("model", ""),
                llm_cost=usage["cost_usd"],
                cache_creation_tokens=usage.get("cache_creation_input_tokens", 0),
                cache_read_tokens=usage.get("cache_read_input_tokens", 0),
                input_tokens=usage["input_tokens"],
                output_tokens=usage["output_tokens"],
            )
        except Exception as e:
            print(f"[AnalyzeAll][cache_store] non-fatal: {e}")

        # Store vector in Qdrant
        if embed_result[0]:
            dbs = get_session()
            gen_meta = None
            try:
                gen_obj = dbs.query(Generation).get(gen_id)
                if gen_obj:
                    gen_meta = gen_obj.to_dict()
            finally:
                dbs.close()
            qdrant_payload = {
                "generation_id": gen_id,
                "user_text_snippet": total_text[:200],
                "topic": parsed.get("topic", ""),
                "is_work": parsed.get("is_work", True),
                "session_id": gen_meta.get("session_id", "") if gen_meta else "",
                "api_key_name": gen_meta.get("sourceKeyName", "") if gen_meta else "",
                "model_used": gen_meta.get("modelDisplayName", "") if gen_meta else "",
                "created_at": gen_meta.get("createdAt", "") if gen_meta else "",
            }
            _qdrant_upsert(gen_id, embed_result[0], qdrant_payload)

        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def _analyze_all_worker():
    """Background thread: analyze all generations without summary."""
    print(f"[AnalyzeAll] started")
    while True:
        with _analyze_all["lock"]:
            if _analyze_all["stop_requested"]:
                _analyze_all["running"] = False
                print("[AnalyzeAll] stopped by request")
                return

        dbs = get_session()
        try:
            # Find all generation IDs without summary
            all_gen_ids = dbs.query(Generation.id).order_by(Generation.created_at.desc()).all()
            all_ids = [row[0] for row in all_gen_ids]

            # Filter out already-analyzed ones
            cached_ids = set()
            for i in range(0, len(all_ids), 500):
                batch = all_ids[i:i + 500]
                found = gen_summary_get_many(batch)
                cached_ids.update(found.keys())

            uncached = [gid for gid in all_ids if gid not in cached_ids]

            with _analyze_all["lock"]:
                _analyze_all["total"] = len(uncached)

            if not uncached:
                with _analyze_all["lock"]:
                    _analyze_all["running"] = False
                    _analyze_all["last_update"] = datetime.now(timezone.utc).isoformat()
                print(f"[AnalyzeAll] done — all generations already analyzed")
                return

            print(f"[AnalyzeAll] {len(uncached)} generations to analyze (of {len(all_ids)} total)")

            for gen_id in uncached:
                with _analyze_all["lock"]:
                    if _analyze_all["stop_requested"]:
                        _analyze_all["running"] = False
                        print("[AnalyzeAll] stopped mid-batch")
                        return
                    # Pause support: wait while paused
                    while _analyze_all["paused"] and not _analyze_all["stop_requested"]:
                        _analyze_all["lock"].release()
                        time.sleep(0.5)
                        _analyze_all["lock"].acquire()
                    if _analyze_all["stop_requested"]:
                        _analyze_all["running"] = False
                        return

                result = _analyze_single_gen(gen_id)

                with _analyze_all["lock"]:
                    if result["status"] == "ok":
                        _analyze_all["done"] += 1
                    elif result["status"] == "skipped":
                        _analyze_all["skipped"] += 1
                    else:
                        _analyze_all["errors"] += 1
                    _analyze_all["last_update"] = datetime.now(timezone.utc).isoformat()

                # Small delay between requests (be polite to APIs)
                time.sleep(0.3)

        except Exception as e:
            with _analyze_all["lock"]:
                _analyze_all["running"] = False
                _analyze_all["last_update"] = datetime.now(timezone.utc).isoformat()
            print(f"[AnalyzeAll] fatal error: {e}")
            return
        finally:
            dbs.close()
        break

    with _analyze_all["lock"]:
        _analyze_all["running"] = False
    print(f"[AnalyzeAll] completed: done={_analyze_all['done']} skipped={_analyze_all['skipped']} errors={_analyze_all['errors']}")


@app.route("/api/analyze-all/start", methods=["POST"])
def api_analyze_all_start():
    """Start background analysis of all unanalyzed generations."""
    with _analyze_all["lock"]:
        if _analyze_all["running"]:
            return jsonify({"status": "already_running"})
        _analyze_all["running"] = True
        _analyze_all["paused"] = False
        _analyze_all["done"] = 0
        _analyze_all["skipped"] = 0
        _analyze_all["total"] = 0
        _analyze_all["errors"] = 0
        _analyze_all["stop_requested"] = False
        _analyze_all["started_at"] = datetime.now(timezone.utc).isoformat()
        _analyze_all["last_update"] = _analyze_all["started_at"]

        t = threading.Thread(target=_analyze_all_worker, daemon=True)
        _analyze_all["thread"] = t
        t.start()

    provider = _provider_state["provider"]
    print(f"[AnalyzeAll] started with provider={provider}")
    return jsonify({"status": "started", "provider": provider})


@app.route("/api/analyze-all/status")
def api_analyze_all_status():
    """Poll analyze-all progress."""
    with _analyze_all["lock"]:
        return jsonify({
            "running": _analyze_all["running"],
            "paused": _analyze_all["paused"],
            "done": _analyze_all["done"],
            "skipped": _analyze_all["skipped"],
            "total": _analyze_all["total"],
            "errors": _analyze_all["errors"],
            "startedAt": _analyze_all["started_at"],
            "lastUpdate": _analyze_all["last_update"],
        })


@app.route("/api/analyze-all/stop", methods=["POST"])
def api_analyze_all_stop():
    """Request analyze-all to stop."""
    with _analyze_all["lock"]:
        _analyze_all["stop_requested"] = True
        _analyze_all["paused"] = False  # unpause so it can see stop flag
    return jsonify({"status": "stopping"})


@app.route("/api/analyze-all/pause", methods=["POST"])
def api_analyze_all_pause():
    """Pause/resume analyze-all."""
    with _analyze_all["lock"]:
        if not _analyze_all["running"]:
            return jsonify({"error": "not running"}), 400
        _analyze_all["paused"] = not _analyze_all["paused"]
        state = "paused" if _analyze_all["paused"] else "resumed"
    print(f"[AnalyzeAll] {state}")
    return jsonify({"status": state})
    return jsonify({"status": "stopping"})


def _summarize_all_worker(employee: str):
    """Background thread: summarize all sessions without summary for an employee."""
    print(f"[Summarizer][summarize_all_worker] started for {employee}")
    while True:
        with _summarize['lock']:
            if _summarize['stop_requested']:
                _summarize['running'] = False
                print("[Summarizer] Stopped by request")
                return

        dbs = get_session()
        try:
            # Find sessions without summary for this employee
            sessions_with_gens = dbs.query(
                Generation.session_id,
            ).filter(
                Generation.source_key_name == employee,
                Generation.session_id.isnot(None),
                Generation.session_id != "",
            ).group_by(Generation.session_id).all()

            # Filter out sessions that already have summaries
            uncached = []
            for (sid,) in sessions_with_gens:
                if not summary_get_or_none(sid):
                    uncached.append(sid)

            if not uncached:
                with _summarize['lock']:
                    _summarize['running'] = False
                    _summarize['last_update'] = datetime.now(timezone.utc).isoformat()
                print(f"[Summarizer][summarize_all_worker] done — no more sessions for {employee}")
                return

            with _summarize['lock']:
                _summarize['total'] = len(uncached) + _summarize['done']

            for sid in uncached:
                with _summarize['lock']:
                    if _summarize['stop_requested']:
                        _summarize['running'] = False
                        print("[Summarizer] Stopped mid-batch")
                        return

                try:
                    _summarize_single_session(sid)
                    with _summarize['lock']:
                        _summarize['done'] += 1
                        _summarize['last_update'] = datetime.now(timezone.utc).isoformat()
                except Exception as e:
                    with _summarize['lock']:
                        _summarize['errors'] += 1
                        _summarize['last_update'] = datetime.now(timezone.utc).isoformat()
                    print(f"[Summarizer][summarize_all_worker] session {sid[:16]} error: {e}")

                time.sleep(1)  # polite delay between LLM calls

        except Exception as e:
            with _summarize['lock']:
                _summarize['error_msg'] = str(e)
                _summarize['running'] = False
            print(f"[Summarizer][summarize_all_worker] fatal: {e}")
            return
        finally:
            dbs.close()

        break  # one pass is enough

    with _summarize['lock']:
        _summarize['running'] = False
    print(f"[Summarizer][summarize_all_worker] completed for {employee}")


@app.route("/api/session/summarize-all", methods=["POST"])
def api_session_summarize_all():
    """Start background summarization for all sessions of an employee."""
    employee = request.args.get("employee") or request.json.get("employee", "")
    if not employee:
        return jsonify({"error": "employee required"}), 400

    with _summarize['lock']:
        if _summarize['running']:
            return jsonify({"status": "already_running", "employee": _summarize['employee']})
        _summarize['running'] = True
        _summarize['done'] = 0
        _summarize['total'] = 0
        _summarize['errors'] = 0
        _summarize['error_msg'] = None
        _summarize['stop_requested'] = False
        _summarize['started_at'] = datetime.now(timezone.utc).isoformat()
        _summarize['last_update'] = _summarize['started_at']
        _summarize['employee'] = employee

        t = threading.Thread(target=_summarize_all_worker, args=(employee,), daemon=True)
        _summarize['thread'] = t
        t.start()

    return jsonify({"status": "started", "employee": employee})


@app.route("/api/session/summarize/status")
def api_session_summarize_status():
    """Poll summarize-all progress."""
    with _summarize['lock']:
        return jsonify({
            "running": _summarize['running'],
            "done": _summarize['done'],
            "total": _summarize['total'],
            "errors": _summarize['errors'],
            "errorMsg": _summarize['error_msg'],
            "lastUpdate": _summarize['last_update'],
            "startedAt": _summarize['started_at'],
            "employee": _summarize['employee'],
        })


@app.route("/api/session/summarize-all/stop", methods=["POST"])
def api_session_summarize_stop():
    """Request summarize-all to stop."""
    with _summarize['lock']:
        _summarize['stop_requested'] = True
    return jsonify({"status": "stopping"})

# ─── END_BLOCK_SESSION_SUMMARIZER


# ─── Main ─────────────────────────────────────────────────────────────────────────

def main():
    global AUTH_TOKEN, sync_worker
    global LLM_API_URL, LLM_MODEL, LLM_API_KEY
    global LLM_PROVIDER, OLLAMA_BASE_URL, OLLAMA_CHAT_MODEL, OLLAMA_EMBED_MODEL
    global OLLAMA_THINKING, OLLAMA_TIMEOUT
    global QDRANT_URL, QDRANT_COLLECTION, QDRANT_ENABLED

    load_env()
    parser = argparse.ArgumentParser(description="Polza.AI Dashboard v3")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    AUTH_TOKEN = os.environ.get("POLZA_API_KEY", "")

    # Cloud (Anthropic) config
    LLM_API_URL = os.environ.get("LLM_API_URL", LLM_API_URL)
    LLM_MODEL = os.environ.get("LLM_MODEL", LLM_MODEL)
    LLM_API_KEY = os.environ.get("LLM_API_KEY", "")

    # On-prem (Ollama) config
    LLM_PROVIDER = os.environ.get("LLM_PROVIDER", LLM_PROVIDER)
    OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", OLLAMA_BASE_URL)
    OLLAMA_CHAT_MODEL = os.environ.get("OLLAMA_CHAT_MODEL", OLLAMA_CHAT_MODEL)
    OLLAMA_EMBED_MODEL = os.environ.get("OLLAMA_EMBED_MODEL", OLLAMA_EMBED_MODEL)
    OLLAMA_THINKING = os.environ.get("OLLAMA_THINKING", "").lower() in ("true", "1", "yes")
    OLLAMA_TIMEOUT = int(os.environ.get("OLLAMA_TIMEOUT", OLLAMA_TIMEOUT))

    # Qdrant config
    QDRANT_URL = os.environ.get("QDRANT_URL", QDRANT_URL)
    QDRANT_COLLECTION = os.environ.get("QDRANT_COLLECTION", QDRANT_COLLECTION)
    QDRANT_ENABLED = os.environ.get("QDRANT_ENABLED", "true").lower() in ("true", "1", "yes")

    # Set runtime provider from env
    _provider_state["provider"] = LLM_PROVIDER
    auto_analyze_env = os.environ.get("AUTO_ANALYZE", "false").lower() in ("true", "1", "yes")
    _provider_state["auto_analyze"] = auto_analyze_env

    provider_icon = "🏠 On-Prem" if LLM_PROVIDER == "ollama" else "☁️ Cloud"
    print(f"🧠 LLM provider: {provider_icon} ({LLM_PROVIDER})")
    if LLM_PROVIDER == "ollama":
        print(f"   Ollama: {OLLAMA_BASE_URL}, model={OLLAMA_CHAT_MODEL}, embed={OLLAMA_EMBED_MODEL}")
        print(f"   Thinking: {'ON' if OLLAMA_THINKING else 'OFF'}, timeout={OLLAMA_TIMEOUT}s")
    else:
        print(f"   Anthropic: url={LLM_API_URL}, model={LLM_MODEL}, key={'✅' if LLM_API_KEY else '❌'}")
    print(f"📦 Qdrant: {QDRANT_URL}/{QDRANT_COLLECTION} ({'enabled' if QDRANT_ENABLED else 'disabled'})")

    # Init DB
    init_db()
    print("✅ PostgreSQL connected")

    # Init Qdrant
    if QDRANT_ENABLED:
        if _qdrant_ensure_collection():
            print(f"✅ Qdrant collection '{QDRANT_COLLECTION}' ready")
        else:
            print(f"⚠️ Qdrant init failed — embeddings will be skipped")

    # Register primary key from .env
    if AUTH_TOKEN:
        session = get_session()
        try:
            existing = session.query(ApiKey).filter(
                ApiKey.token == AUTH_TOKEN
            ).first()
            if not existing:
                pk = ApiKey(
                    name="Основной ключ", token=AUTH_TOKEN,
                    key_suffix=AUTH_TOKEN[-6:], is_primary=True
                )
                session.add(pk)
                session.commit()
                print(f"✅ Primary key registered: ...{AUTH_TOKEN[-6:]}")
            else:
                print(f"✅ Primary key exists: {existing.name}")
        finally:
            session.close()

    # Register keys from POLZA_API_KEYS env var
    raw_keys = os.environ.get("POLZA_API_KEYS", "")
    if raw_keys:
        parsed = parse_keys_text(raw_keys)
        session = get_session()
        try:
            added = 0
            for k in parsed:
                existing = session.query(ApiKey).filter(
                    ApiKey.token == k["key"]
                ).first()
                if not existing:
                    ak = ApiKey(
                        name=k["name"], token=k["key"],
                        key_suffix=k["key"][-6:], is_primary=False
                    )
                    session.add(ak)
                    added += 1
            session.commit()
            if added:
                print(f"✅ {added} additional keys from .env")
        finally:
            session.close()

    # Start background sync worker
    sync_worker = SyncWorker()
    sync_worker.start()

    session = get_session()
    total_keys = session.query(ApiKey).count()
    total_gens = session.query(Generation).count()
    session.close()

    print(f"\n🚀 Polza.AI Dashboard v3")
    print(f"   {total_keys} keys  |  {total_gens} cached records")
    print(f"   Sync every {SYNC_INTERVAL // 60} min  |  http://{args.host}:{args.port}\n")

    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()
