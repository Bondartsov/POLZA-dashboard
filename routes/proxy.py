from flask import Blueprint, jsonify

from config import (
    get_session, ApiKey, Generation, AUTH_TOKEN, _resolve_token_for_gen,
    _headers, POLZA_API, sync_worker,
)
import requests as http_requests

proxy_bp = Blueprint('proxy', __name__)


@proxy_bp.route("/api/generations/<gen_id>")
def api_detail(gen_id):
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


@proxy_bp.route("/api/generations/<gen_id>/log")
def api_log(gen_id):
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


@proxy_bp.route("/api/balance")
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
