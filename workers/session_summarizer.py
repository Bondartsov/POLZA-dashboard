import json
import threading
import time
from datetime import datetime, timezone

import requests as http_requests

from config import (
    get_session, ApiKey, Generation, AUTH_TOKEN, LLM_API_URL, LLM_API_KEY, LLM_MODEL,
    _resolve_token_for_gen, _headers, _provider_state, OLLAMA_TIMEOUT, POLZA_API,
    summary_get_or_none, summary_upsert,
)

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


def _summarize_single_session(session_id: str):
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
        ).order_by(Generation.created_at_api.desc()).all()

        if not gens:
            raise ValueError("NO_GENERATIONS")

        source_key = gens[0].source_key_name or ""

        key_row = dbs.query(ApiKey).filter(
            ApiKey.name == source_key
        ).first()
        token = key_row.token if key_row else AUTH_TOKEN

        user_messages = []
        for gen in gens[:20]:
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

    total_text = "\n---\n".join(user_messages)[:3000]

    # START_BLOCK_CALL_LLM
    llm_payload = {
        "model": LLM_MODEL,
        "max_tokens": 1000,
        "temperature": 0.3,
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
    json_str = content.strip()
    if json_str.startswith("```"):
        lines = json_str.split("\n")
        json_str = "\n".join(lines[1:-1])
    if json_str.startswith("json"):
        json_str = json_str[4:].strip()

    parsed = json.loads(json_str)
    print(f"[Summarizer][summarize_session][BLOCK_PARSE_RESPONSE] parsed OK: topic={parsed.get('topic', '?')}")
    # END_BLOCK_PARSE_RESPONSE

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


def _summarize_all_worker(employee: str):
    print(f"[Summarizer][summarize_all_worker] started for {employee}")
    while True:
        with _summarize['lock']:
            if _summarize['stop_requested']:
                _summarize['running'] = False
                print("[Summarizer] Stopped by request")
                return

        dbs = get_session()
        try:
            sessions_with_gens = dbs.query(
                Generation.session_id,
            ).filter(
                Generation.source_key_name == employee,
                Generation.session_id.isnot(None),
                Generation.session_id != "",
            ).group_by(Generation.session_id).all()

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

                time.sleep(1)

        except Exception as e:
            with _summarize['lock']:
                _summarize['error_msg'] = str(e)
                _summarize['running'] = False
            print(f"[Summarizer][summarize_all_worker] fatal: {e}")
            return
        finally:
            dbs.close()

        break

    with _summarize['lock']:
        _summarize['running'] = False
    print(f"[Summarizer][summarize_all_worker] completed for {employee}")
