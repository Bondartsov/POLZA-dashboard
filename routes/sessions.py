import json
import threading
import time
from datetime import datetime, timezone
from flask import Blueprint, jsonify, request
from sqlalchemy import func, desc

from config import (
    get_session, Generation, ApiKey, AUTH_TOKEN, _resolve_token_for_gen, _headers,
    POLZA_API, summary_get_or_none, summary_upsert,
)
from routes.generations import _apply_filters
from workers.session_summarizer import _summarize, _summarize_single_session, _summarize_all_worker
import requests as http_requests

sessions_bp = Blueprint('sessions', __name__)

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

                if batch_errors >= 45:
                    _backfill['error_msg'] = f"Слишком много ошибок API ({batch_errors}/50 в последней партии)"
                    _backfill['running'] = False
                    print(f"[Backfill] Paused: {_backfill['error_msg']}")
                    return

            time.sleep(0.5)

        except Exception as e:
            with _backfill['lock']:
                _backfill['error_msg'] = str(e)
                _backfill['running'] = False
            print(f"[Backfill] Fatal error: {e}")
            return
        finally:
            dbs.close()


@sessions_bp.route("/api/sessions/backfill/start", methods=["POST"])
def api_backfill_start():
    with _backfill['lock']:
        if _backfill['running']:
            return jsonify({"status": "already_running"})
        _backfill['running'] = True
        _backfill['enriched'] = 0
        _backfill['no_data'] = 0
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
            _backfill['total'] = _backfill['remaining']
        finally:
            dbs.close()

        t = threading.Thread(target=_backfill_worker, daemon=True)
        _backfill['thread'] = t
        t.start()

    return jsonify({"status": "started", "remaining": _backfill['remaining']})


@sessions_bp.route("/api/sessions/backfill/stop", methods=["POST"])
def api_backfill_stop():
    with _backfill['lock']:
        _backfill['stop_requested'] = True
    return jsonify({"status": "stopping"})


@sessions_bp.route("/api/sessions/backfill/retry", methods=["POST"])
def api_backfill_retry():
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


@sessions_bp.route("/api/sessions/backfill/status")
def api_backfill_status():
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


@sessions_bp.route("/api/db/sessions")
def api_db_sessions():
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


@sessions_bp.route("/api/session/summarize", methods=["POST", "GET"])
def api_session_summarize():
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


@sessions_bp.route("/api/session/summarize-all", methods=["POST"])
def api_session_summarize_all():
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


@sessions_bp.route("/api/session/summarize/status")
def api_session_summarize_status():
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


@sessions_bp.route("/api/session/summarize-all/stop", methods=["POST"])
def api_session_summarize_stop():
    with _summarize['lock']:
        _summarize['stop_requested'] = True
    return jsonify({"status": "stopping"})
