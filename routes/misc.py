from flask import Blueprint, jsonify, send_from_directory

from config import get_session, ApiKey, Generation, AUTH_TOKEN, sync_worker, STATIC_DIR

misc_bp = Blueprint('misc', __name__)


@misc_bp.route("/")
def index():
    return send_from_directory(str(STATIC_DIR), "index.html")


@misc_bp.route("/api/config")
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


@misc_bp.route("/api/health")
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
