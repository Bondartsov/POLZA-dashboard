#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polza.AI Dashboard v3 — Flask backend with PostgreSQL caching.
Запуск: python polza_dashboard.py [--port 5000] [--debug]

Architecture:
  PostgreSQL (api_keys, generations) <- sync_worker (every 5 min) <- Polza.AI API
  Frontend reads from DB (fast), detail/log proxied through API (needs correct token)
"""
import argparse, json, os, sys
from pathlib import Path
from datetime import datetime, timezone
from flask import Flask, jsonify, request, send_from_directory
import requests as http_requests
from sqlalchemy import func, desc, asc

if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
POLZA_API = "https://polza.ai/api/v1"
SYNC_INTERVAL = 300  # 5 minutes

from db import init_db, get_session, ApiKey, Generation, engine
from sync_worker import SyncWorker, sync_all_keys

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="")
sync_worker = None
AUTH_TOKEN = ""


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


# ─── Main ─────────────────────────────────────────────────────────────────────────

def main():
    global AUTH_TOKEN, sync_worker

    load_env()
    parser = argparse.ArgumentParser(description="Polza.AI Dashboard v3")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    AUTH_TOKEN = os.environ.get("POLZA_API_KEY", "")

    # Init DB
    init_db()
    print("✅ PostgreSQL connected")

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
