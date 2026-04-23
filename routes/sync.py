from flask import Blueprint, jsonify

from config import sync_worker, sync_all_keys

sync_bp = Blueprint('sync', __name__)


@sync_bp.route("/api/sync", methods=["POST"])
def api_trigger_sync():
    if sync_worker:
        sync_worker.trigger()
        return jsonify({"status": "triggered"})
    return jsonify({"error": "Sync worker not running"}), 500


@sync_bp.route("/api/sync/run", methods=["POST"])
def api_sync_run():
    try:
        results, new_ids = sync_all_keys()
        total_new = sum(r["new"] for r in results)
        return jsonify({"results": results, "totalNew": total_new})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@sync_bp.route("/api/sync/status")
def api_sync_status():
    if sync_worker:
        return jsonify(sync_worker.status())
    return jsonify({"status": "not running"})
