# FILE: routes/chat.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
#   PURPOSE: Flask Blueprint for RAG chat endpoints — SSE streaming, session management
#   SCOPE: POST /api/chat/message (SSE), POST /api/chat/new, GET /api/chat/status
#   DEPENDS: M-CONFIG, M-RAG-CHAT
#   LINKS: M-RAG-CHAT
# END_MODULE_CONTRACT

import json
from flask import Blueprint, request, Response, jsonify

from config import app
from rag.chat import chat_send, _chat_new_session, _get_chat_model
from embeddings.qdrant import _get_qdrant_client, QDRANT_COLLECTION

chat_bp = Blueprint("chat", __name__)


@chat_bp.route("/api/chat/message", methods=["POST"])
def api_chat_message():
    """SSE streaming endpoint for RAG chat. Returns text/event-stream."""
    data = request.get_json(silent=True) or {}
    session_id = data.get("session_id", "")
    message = data.get("message", "").strip()

    if not message:
        return jsonify({"error": "Empty message"}), 400

    # Truncate very long messages
    if len(message) > 1000:
        message = message[:1000]

    def generate():
        for event in chat_send(session_id, message):
            if isinstance(event, str):
                yield event.encode("utf-8")
            else:
                yield event

    return Response(generate(), content_type="text/event-stream; charset=utf-8",
                    headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@chat_bp.route("/api/chat/new", methods=["POST"])
def api_chat_new():
    """Create new chat session."""
    session_id = _chat_new_session()
    return jsonify({"session_id": session_id})


@chat_bp.route("/api/chat/status", methods=["GET"])
def api_chat_status():
    """Return RAG chat status: vector count, model info, availability."""
    vectors = 0
    qdrant_ok = False
    try:
        client = _get_qdrant_client()
        if client:
            info = client.get_collection(QDRANT_COLLECTION)
            vectors = getattr(info, "points_count", 0) or 0
            qdrant_ok = True
    except Exception:
        pass

    return jsonify({
        "vectors": vectors,
        "model": _get_chat_model(),
        "qdrant_ok": qdrant_ok,
        "available": qdrant_ok and vectors > 0,
    })
