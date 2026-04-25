# FILE: embeddings/payload.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
#   PURPOSE: Build complete Qdrant payload from Generation model + LLM analysis results
#   SCOPE: Field mapping, type coercion, payload construction
#   DEPENDS: M-DB (Generation model)
#   LINKS: M-EMBEDDING
# END_MODULE_CONTRACT


def _build_qdrant_payload(gen_meta: dict, analysis: dict, user_text_snippet: str = "") -> dict:
    """Build complete Qdrant payload from Generation.to_dict() + LLM analysis.

    Maps ALL fields from the Generation model (synced from Polza.AI API)
    plus LLM-generated analysis (topic, summary, is_work, etc.) into a flat
    dictionary suitable for Qdrant point payload.

    Args:
        gen_meta: dict from Generation.to_dict() — contains all API fields
        analysis: dict from LLM summarization — {topic, summary, is_work, project_guess, risk_flags}
        user_text_snippet: first 200 chars of user's prompt text (for preview)

    Returns:
        Flat dict with snake_case keys for Qdrant storage.
    """
    usage = gen_meta.get("usage") or {}

    payload = {
        # ─── Primary key ───
        "generation_id": gen_meta.get("id", ""),

        # ─── User content ───
        "user_text_snippet": (user_text_snippet or "")[:200],

        # ─── LLM Analysis (from generation_summaries) ───
        "topic": analysis.get("topic", ""),
        "summary": analysis.get("summary", ""),
        "is_work": analysis.get("is_work", True),
        "project_guess": analysis.get("project_guess", ""),
        "risk_flags": analysis.get("risk_flags", []),

        # ─── Model info ───
        "model": gen_meta.get("model", ""),
        "model_display_name": gen_meta.get("modelDisplayName", ""),

        # ─── Request metadata ───
        "request_type": gen_meta.get("requestType", ""),
        "status": gen_meta.get("status", ""),
        "finish_reason": gen_meta.get("finishReason", ""),
        "response_mode": gen_meta.get("responseMode", ""),

        # ─── Cost & tokens ───
        "cost": _safe_float(gen_meta.get("cost")),
        "client_cost": _safe_float(gen_meta.get("clientCost")),
        "prompt_tokens": _safe_int(gen_meta.get("promptTokens") or usage.get("promptTokens")),
        "completion_tokens": _safe_int(gen_meta.get("completionTokens") or usage.get("completionTokens")),
        "total_tokens": _safe_int(gen_meta.get("totalTokens") or usage.get("totalTokens")),
        "cached_tokens": _safe_int(usage.get("cachedTokens")),
        "reasoning_tokens": _safe_int(usage.get("reasoningTokens")),
        "audio_tokens": _safe_int(usage.get("audioTokens")),
        "video_tokens": _safe_int(usage.get("videoTokens")),

        # ─── Performance ───
        "generation_time_ms": _safe_int(gen_meta.get("generationTimeMs")),
        "latency_ms": _safe_int(gen_meta.get("latencyMs")),

        # ─── Timestamps ───
        "created_at": gen_meta.get("createdAt", ""),
        "completed_at": gen_meta.get("completedAt", ""),

        # ─── API key / Employee ───
        "api_key_name": gen_meta.get("apiKeyName", ""),
        "api_key_short": gen_meta.get("apiKeyShort", ""),
        "api_key_id": gen_meta.get("apiKeyId", ""),

        # ─── Routing ───
        "final_endpoint_slug": gen_meta.get("finalEndpointSlug", ""),
        "api_type": gen_meta.get("apiType", ""),
        "provider": gen_meta.get("provider", ""),

        # ─── Session / Device ───
        "session_id": gen_meta.get("_sessionId", ""),
        "device_id": gen_meta.get("_deviceId", ""),
        "source_key_name": gen_meta.get("_sourceKey", ""),
        "has_log": bool(gen_meta.get("hasLog", False)),
    }

    return payload


def _safe_float(v) -> float:
    """Convert to float, defaulting to 0.0 for None/invalid."""
    if v is None:
        return 0.0
    try:
        return float(v)
    except (ValueError, TypeError):
        return 0.0


def _safe_int(v) -> int:
    """Convert to int, defaulting to 0 for None/invalid."""
    if v is None:
        return 0
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0
