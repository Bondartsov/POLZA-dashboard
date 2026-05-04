import json
from flask import Blueprint, jsonify, request

from config import (
    get_session, Generation, _resolve_token_for_gen, _headers, _provider_state,
    OLLAMA_CHAT_MODEL, OLLAMA_TIMEOUT, POLZA_API,
    gen_summary_get_or_none, gen_summary_get_many, gen_summary_upsert, gen_summary_delete,
)
import requests as http_requests
from providers.dispatcher import _llm_call_summarize

summarize_bp = Blueprint('summarize', __name__)


def _extract_user_text_from_log(log_data: dict, limit_chars: int = 4000) -> str:
    msgs = log_data.get("request", {}).get("messages", [])
    user_parts = []
    for m in msgs:
        role = m.get("role", "")
        content = m.get("content", "")
        if role == "user" and content:
            user_parts.append(str(content)[:1200])
    total_text = "\n---\n".join(user_parts)[:limit_chars]
    if not total_text.strip():
        for m in msgs:
            content = m.get("content", "")
            if content:
                user_parts.append(str(content)[:1200])
        total_text = "\n---\n".join(user_parts)[:limit_chars]
    return total_text.strip()


@summarize_bp.route("/api/generation/summarize", methods=["POST"])
def api_generation_summarize():
    data = request.get_json(silent=True) or {}
    gen_id = data.get("generationId", "")
    force = bool(data.get("force") or request.args.get("force"))
    if not gen_id:
        return jsonify({"error": "generationId required"}), 400

    if not force:
        cached = gen_summary_get_or_none(gen_id)
        if cached:
            print(f"[GenSummarize][cache_hit] generation_id={gen_id[:16]}")
            return jsonify(cached.to_dict())

    try:
        token = _resolve_token_for_gen(gen_id)

        r = http_requests.get(
            f"{POLZA_API}/history/generations/{gen_id}/log",
            headers=_headers(token), timeout=30,
        )
        if r.status_code != 200:
            return jsonify({
                "topic": "Лог недоступен",
                "summary": f"Не удалось получить лог генерации (HTTP {r.status_code}).",
                "isWork": True, "generationId": gen_id, "cached": False,
            }), 200

        total_text = _extract_user_text_from_log(r.json(), limit_chars=4000)

        if not total_text:
            return jsonify({
                "topic": "Пустой запрос",
                "summary": "В логе нет текста промпта для анализа.",
                "isWork": True, "generationId": gen_id, "cached": False,
            }), 200

        provider = _provider_state["provider"]
        active_model = OLLAMA_CHAT_MODEL if provider == "ollama" else ""
        print(f"[GenSummarize][LLM] provider={provider} model={active_model} text_chars={len(total_text)}")

        parsed, usage = _llm_call_summarize(total_text)
        print(
            f"[GenSummarize][LLM] ok provider={provider} "
            f"input={usage['input_tokens']} output={usage['output_tokens']} "
            f"cost=${usage['cost_usd']:.6f}"
        )

        try:
            gen_summary_upsert(
                generation_id=gen_id,
                summary=parsed.get("summary", ""),
                topic=parsed.get("topic", ""),
                is_work=parsed.get("is_work", True),
                project_guess=parsed.get("project_guess"),
                risk_flags=parsed.get("risk_flags", []),
                llm_model=usage.get("model", active_model),
                llm_cost=usage["cost_usd"],
                cache_creation_tokens=usage.get("cache_creation_input_tokens", 0),
                cache_read_tokens=usage.get("cache_read_input_tokens", 0),
                input_tokens=usage["input_tokens"],
                output_tokens=usage["output_tokens"],
            )
        except Exception as e:
            print(f"[GenSummarize][cache_store] non-fatal: {e}")

        return jsonify({
            "generationId": gen_id,
            "summary": parsed.get("summary", ""),
            "topic": parsed.get("topic", ""),
            "isWork": parsed.get("is_work", True),
            "projectGuess": parsed.get("project_guess"),
            "riskFlags": parsed.get("risk_flags", []),
            "llmModel": usage.get("model", active_model),
            "llmCost": usage["cost_usd"],
            "cacheCreationTokens": usage.get("cache_creation_input_tokens", 0),
            "cacheReadTokens": usage.get("cache_read_input_tokens", 0),
            "inputTokens": usage["input_tokens"],
            "outputTokens": usage["output_tokens"],
            "provider": provider,
            "cached": False,
        })

    except json.JSONDecodeError as e:
        print(f"[GenSummarize] JSON decode error: {e}")
        return jsonify({
            "topic": "Ошибка разбора ответа LLM",
            "summary": "Модель вернула невалидный JSON — попробуйте ещё раз.",
            "isWork": True, "generationId": gen_id, "cached": False,
            "provider": _provider_state.get("provider", "unknown"),
        }), 200
    except Exception as e:
        print(f"[GenSummarize] ERROR: {e}")
        return jsonify({
            "error": str(e),
            "topic": "Ошибка",
            "summary": f"{e}",
            "generationId": gen_id,
            "cached": False,
        }), 500


@summarize_bp.route("/api/generation-summaries", methods=["GET", "POST"])
def api_generation_summaries_batch():
    if request.method == "POST":
        data = request.get_json(silent=True) or {}
        ids = data.get("ids") or []
    else:
        raw = request.args.get("ids", "")
        ids = [x.strip() for x in raw.split(",") if x.strip()]

    if not ids:
        return jsonify({"summaries": {}})

    ids = ids[:500]
    try:
        found = gen_summary_get_many(ids)
        return jsonify({"summaries": found})
    except Exception as e:
        print(f"[GenSummaries][batch] ERROR: {e}")
        return jsonify({"summaries": {}, "error": str(e)}), 500


@summarize_bp.route("/api/generation/summary", methods=["DELETE"])
def api_generation_summary_delete():
    gen_id = request.args.get("generationId") or (request.get_json(silent=True) or {}).get("generationId", "")
    if not gen_id:
        return jsonify({"error": "generationId required"}), 400
    try:
        gen_summary_delete(gen_id)
        return jsonify({"ok": True, "generationId": gen_id})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
