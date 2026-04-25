# FILE: rag/chat.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
#   PURPOSE: RAG chat engine — session management, prompt assembly, OpenRouter streaming via SSE
#   SCOPE: In-memory chat sessions, RAG prompt builder, Nemotron Nano streaming call
#   DEPENDS: M-CONFIG, M-RAG-SEARCH
#   LINKS: M-RAG-CHAT
# END_MODULE_CONTRACT

import uuid
import json
import time
import threading
from datetime import datetime, timezone
from collections import OrderedDict

import requests as http_requests
import config
from rag.search import _rag_search
from rag.prompts import RAG_SYSTEM_PROMPT

# In-memory chat sessions: {session_id: {"messages": [...], "created_at": datetime}}
_chat_sessions = OrderedDict()
_chat_lock = threading.Lock()

# Config defaults
_CHAT_MODEL = "nvidia/nemotron-3-nano-30b-a3b:free"
_MAX_HISTORY = 20
_SESSION_TTL = 7200  # 2 hours
_MAX_TOKENS = 2000
_TEMPERATURE = 0.3
_MAX_RETRIES = 4


def _get_chat_model():
    return getattr(config, "RAG_CHAT_MODEL", _CHAT_MODEL)


def _get_max_history():
    return getattr(config, "RAG_MAX_HISTORY", _MAX_HISTORY)


def _get_session_ttl():
    return getattr(config, "RAG_SESSION_TTL", _SESSION_TTL)


# START_BLOCK_CHAT_SESSIONS
def _chat_new_session() -> str:
    """Create new chat session. Returns session_id (UUID)."""
    session_id = str(uuid.uuid4())
    with _chat_lock:
        _chat_sessions[session_id] = {
            "messages": [],
            "created_at": datetime.now(timezone.utc),
        }
    print(f"[RAG][Chat] new session {session_id[:8]}")
    return session_id


def _chat_add_message(session_id: str, role: str, content: str):
    """Append message to session history. Trim to max history."""
    with _chat_lock:
        if session_id not in _chat_sessions:
            _chat_sessions[session_id] = {
                "messages": [],
                "created_at": datetime.now(timezone.utc),
            }
        session = _chat_sessions[session_id]
        session["messages"].append({"role": role, "content": content})
        # Trim to max history (keep system + last N exchanges)
        max_hist = _get_max_history()
        if len(session["messages"]) > max_hist:
            session["messages"] = session["messages"][-max_hist:]


def _chat_get_session(session_id: str):
    """Get session data or None."""
    with _chat_lock:
        return _chat_sessions.get(session_id)


def _cleanup_expired_sessions():
    """Remove sessions older than TTL."""
    ttl = _get_session_ttl()
    now = datetime.now(timezone.utc)
    with _chat_lock:
        expired = [
            sid for sid, data in _chat_sessions.items()
            if (now - data["created_at"]).total_seconds() > ttl
        ]
        for sid in expired:
            del _chat_sessions[sid]
        if expired:
            print(f"[RAG][Chat] cleaned {len(expired)} expired sessions")
# END_BLOCK_CHAT_SESSIONS


# START_BLOCK_CHAT_PROMPT
def _build_rag_messages(session_history: list, context_block: str, user_message: str) -> list:
    """Build OpenRouter messages array: system + context + history + user message."""
    messages = [
        {"role": "system", "content": RAG_SYSTEM_PROMPT + "\n\n" + context_block},
    ]

    # Add conversation history (skip first system)
    for msg in session_history:
        if msg["role"] in ("user", "assistant"):
            messages.append({"role": msg["role"], "content": msg["content"]})

    # Add current user message (if not already in history)
    if not session_history or session_history[-1].get("content") != user_message:
        messages.append({"role": "user", "content": user_message})

    return messages
# END_BLOCK_CHAT_PROMPT


# START_BLOCK_CHAT_STREAM
def _stream_chat_response(messages: list):
    """Call OpenRouter streaming API with Nemotron Nano. Yields SSE event strings.

    Yields:
        str: SSE formatted events: "data: {json}\n\n"
    """
    model = _get_chat_model()
    url = f"{config.OPENROUTER_BASE_URL}/chat/completions"
    headers = {
        "Authorization": f"Bearer {config.OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://polza-dashboard.local",
        "X-Title": "Polza.AI RAG Chat",
    }
    payload = {
        "model": model,
        "messages": messages,
        "max_tokens": getattr(config, "RAG_MAX_TOKENS", _MAX_TOKENS),
        "temperature": getattr(config, "RAG_TEMPERATURE", _TEMPERATURE),
        "stream": True,
    }

    total_tokens = 0

    r = None
    for attempt in range(_MAX_RETRIES + 1):
        try:
            r = http_requests.post(
                url, headers=headers, json=payload, timeout=120, stream=True
            )
            if r.status_code == 429 and attempt < _MAX_RETRIES:
                wait = 2 ** (attempt + 1)
                print(f"[RAG][Chat] 429 rate-limited, retry {attempt+1}/{_MAX_RETRIES} in {wait}s")
                time.sleep(wait)
                continue
            if r.status_code != 200:
                error_text = r.text[:300]
                print(f"[RAG][Chat] OpenRouter HTTP {r.status_code}: {error_text}")
                yield f"data: {json.dumps({'type': 'error', 'message': f'OpenRouter HTTP {r.status_code}'})}\n\n"
                return
            break
        except Exception as e:
            if attempt < _MAX_RETRIES:
                time.sleep(2 ** (attempt + 1))
                continue
            print(f"[RAG][Chat] connection error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"
            return

    if r is None:
        yield f"data: {json.dumps({'type': 'error', 'message': 'No response from OpenRouter'})}\n\n"
        return

    # Stream response chunks
    try:
        for line in r.iter_lines(decode_unicode=True):
            if not line:
                continue
            if not line.startswith("data: "):
                continue
            data_str = line[6:]
            if data_str.strip() == "[DONE]":
                break
            try:
                chunk = json.loads(data_str)
                choices = chunk.get("choices", [])
                if not choices:
                    continue
                delta = choices[0].get("delta", {})
                content = delta.get("content", "")
                if content:
                    total_tokens += 1
                    yield f"data: {json.dumps({'type': 'token', 'content': content}, ensure_ascii=False)}\n\n"
            except json.JSONDecodeError:
                continue
    except Exception as e:
        print(f"[RAG][Chat] stream error: {e}")
        yield f"data: {json.dumps({'type': 'error', 'message': f'Stream error: {e}'})}\n\n"
        return

    print(f"[RAG][Chat] done tokens≈{total_tokens}")
    yield f"data: {json.dumps({'type': 'done', 'tokens_used': total_tokens})}\n\n"
# END_BLOCK_CHAT_STREAM


# START_BLOCK_CHAT_SEND
def chat_send(session_id: str, message: str):
    """Full RAG chat pipeline: search → build context → stream response.

    Yields SSE event strings for Flask Response.
    """
    # Ensure session exists
    if not session_id:
        session_id = _chat_new_session()

    session = _chat_get_session(session_id)
    if not session:
        session_id = _chat_new_session()
        session = _chat_get_session(session_id)

    # Cleanup old sessions periodically
    _cleanup_expired_sessions()

    # Step 1: RAG search
    search_result = _rag_search(message)

    if search_result.get("error") and not search_result.get("sources"):
        yield f"data: {json.dumps({'type': 'error', 'message': search_result['error']}, ensure_ascii=False)}\n\n"
        yield f"data: {json.dumps({'type': 'done', 'tokens_used': 0})}\n\n"
        return

    # Step 2: Send sources event
    sources_brief = []
    for src in search_result.get("sources", []):
        sources_brief.append({
            "id": src.get("generation_id", "")[:16],
            "topic": src.get("topic", ""),
            "employee": src.get("employee", ""),
            "score": round(src.get("score", 0), 2),
        })

    yield f"data: {json.dumps({'type': 'sources', 'count': search_result['count'], 'data': sources_brief}, ensure_ascii=False)}\n\n"

    # Step 3: Add user message to history
    _chat_add_message(session_id, "user", message)

    # Step 4: Build RAG messages
    session = _chat_get_session(session_id)
    history = session.get("messages", []) if session else []
    context_block = search_result.get("context_block", "")
    messages = _build_rag_messages(history[:-1], context_block, message)  # Exclude last (current) from history

    # Step 5: Stream LLM response
    full_response = []
    for event in _stream_chat_response(messages):
        yield event
        # Collect tokens for history
        try:
            data = json.loads(event.strip().replace("data: ", "", 1))
            if data.get("type") == "token":
                full_response.append(data.get("content", ""))
        except (json.JSONDecodeError, ValueError):
            pass

    # Step 6: Save assistant response to history
    if full_response:
        _chat_add_message(session_id, "assistant", "".join(full_response))
# END_BLOCK_CHAT_SEND
