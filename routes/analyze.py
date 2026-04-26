import threading
from datetime import datetime, timezone
from flask import Blueprint, jsonify

from config import (
    get_session, Generation, _provider_state,
    get_analysis_state, update_analysis_state, get_analysis_counts,
)
from workers.analyze_all import _analyze_all, _analyze_all_worker
from workers.vectorize_existing import (
    start_vectorize_existing, stop_vectorize_existing, get_vectorize_state,
)

analyze_bp = Blueprint('analyze', __name__)


@analyze_bp.route("/api/analysis-stats")
def api_analysis_stats():
    counts = get_analysis_counts()
    state = get_analysis_state()
    with _analyze_all["lock"]:
        if _analyze_all["running"]:
            return jsonify({
                "total": counts["total"],
                "analyzed": counts["analyzed"],
                "remaining": counts["remaining"],
                "job": {
                    "status": "paused" if _analyze_all["paused"] else "running",
                    "done": _analyze_all["done"],
                    "skipped": _analyze_all["skipped"],
                    "total": _analyze_all["total"],
                    "errors": _analyze_all["errors"],
                    "startedAt": state.started_at.isoformat() if state.started_at else None,
                    "updatedAt": datetime.now(timezone.utc).isoformat(),
                }
            })
    return jsonify({
        "total": counts["total"],
        "analyzed": counts["analyzed"],
        "remaining": counts["remaining"],
        "job": {
            "status": state.status,
            "done": state.done or 0,
            "skipped": state.skipped or 0,
            "total": state.total or 0,
            "errors": state.errors or 0,
            "startedAt": state.started_at.isoformat() if state.started_at else None,
            "updatedAt": state.updated_at.isoformat() if state.updated_at else None,
            "completedAt": state.completed_at.isoformat() if state.completed_at else None,
        }
    })


@analyze_bp.route("/api/analyze-all/start", methods=["POST"])
def api_analyze_all_start():
    with _analyze_all["lock"]:
        if _analyze_all["running"] and not _analyze_all.get("paused"):
            return jsonify({"status": "already_running"})

        # If running but paused — resume by unpausing
        if _analyze_all["running"] and _analyze_all.get("paused"):
            _analyze_all["paused"] = False
            update_analysis_state(status="running")
            print("[AnalyzeAll] resumed from pause")
            return jsonify({"status": "resumed", "provider": _provider_state.get("provider", "ollama")})

        prev_state = get_analysis_state()
        if prev_state.status == "paused" and (prev_state.done or 0) > 0:
            print(f"[AnalyzeAll] resuming paused job (done={prev_state.done})")
            _analyze_all["done"] = 0
            _analyze_all["skipped"] = 0
            _analyze_all["errors"] = prev_state.errors or 0
            _analyze_all["total"] = 0
        else:
            _analyze_all["done"] = 0
            _analyze_all["skipped"] = 0
            _analyze_all["total"] = 0
            _analyze_all["errors"] = 0

        _analyze_all["running"] = True
        _analyze_all["paused"] = False
        _analyze_all["stop_requested"] = False

        update_analysis_state(
            status="running",
            done=0,
            skipped=0,
            errors=0,
            total=0,
            started_at=datetime.now(timezone.utc),
            completed_at=None,
        )

        t = threading.Thread(target=_analyze_all_worker, daemon=True)
        _analyze_all["thread"] = t
        t.start()

    provider = _provider_state["provider"]
    print(f"[AnalyzeAll] started with provider={provider}")
    return jsonify({"status": "started", "provider": provider})


@analyze_bp.route("/api/analyze-all/status")
def api_analyze_all_status():
    counts = get_analysis_counts()
    with _analyze_all["lock"]:
        running = _analyze_all["running"]
        paused = _analyze_all["paused"]
        state = get_analysis_state()
        return jsonify({
            "running": running,
            "paused": paused,
            "done": _analyze_all["done"] if running else (state.done or 0),
            "skipped": _analyze_all["skipped"] if running else (state.skipped or 0),
            "total": _analyze_all["total"] if running else (state.total or 0),
            "errors": _analyze_all["errors"] if running else (state.errors or 0),
            "startedAt": state.started_at.isoformat() if state.started_at else None,
            "lastUpdate": state.updated_at.isoformat() if state.updated_at else None,
            "status": state.status,
            "dbTotal": counts["total"],
            "dbAnalyzed": counts["analyzed"],
            "dbRemaining": counts["remaining"],
        })


@analyze_bp.route("/api/analyze-all/stop", methods=["POST"])
def api_analyze_all_stop():
    with _analyze_all["lock"]:
        _analyze_all["stop_requested"] = True
        _analyze_all["paused"] = False
    update_analysis_state(status="paused")
    return jsonify({"status": "paused"})


@analyze_bp.route("/api/analyze-all/pause", methods=["POST"])
def api_analyze_all_pause():
    with _analyze_all["lock"]:
        if not _analyze_all["running"]:
            return jsonify({"error": "not running"}), 400
        _analyze_all["paused"] = not _analyze_all["paused"]
        state_label = "paused" if _analyze_all["paused"] else "resumed"
    update_analysis_state(status=state_label)
    print(f"[AnalyzeAll] {state_label}")
    return jsonify({"status": state_label})


# ─── Vectorize-Existing endpoints ─────────────────────────────────────────────
@analyze_bp.route("/api/vectorize-existing/start", methods=["POST"])
def api_vectorize_existing_start():
    """Start vectorization of existing summaries (no LLM, only embedding)."""
    if not _provider_state.get("embedding_enabled", False):
        return jsonify({"error": "embedding is disabled — enable it in sidebar first"}), 400
    return jsonify(start_vectorize_existing())


@analyze_bp.route("/api/vectorize-existing/stop", methods=["POST"])
def api_vectorize_existing_stop():
    return jsonify(stop_vectorize_existing())


@analyze_bp.route("/api/vectorize-existing/status")
def api_vectorize_existing_status():
    state = get_vectorize_state()
    # Also return count of unvectorized records
    from config import GenerationSummary, get_session
    s = get_session()
    try:
        unvec_count = s.query(GenerationSummary).filter(
            GenerationSummary.is_vectorized == False
        ).count()
        total_summary_count = s.query(GenerationSummary).count()
    finally:
        s.close()
    
    state["unvectorizedCount"] = unvec_count
    state["totalSummaryCount"] = total_summary_count
    state["vectorizedCount"] = total_summary_count - unvec_count
    return jsonify(state)
