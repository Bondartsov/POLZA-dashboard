from flask import Blueprint, jsonify, request

from config import get_session, ApiKey

keys_bp = Blueprint('keys', __name__)


@keys_bp.route("/api/keys", methods=["POST"])
def api_register_keys():
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
