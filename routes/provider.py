from flask import Blueprint, jsonify, request
import config as _config
from config import (
    _provider_state, _persist_provider_to_env,
    OLLAMA_BASE_URL, OLLAMA_CHAT_MODEL, OLLAMA_EMBED_MODEL, OLLAMA_THINKING,
    OPENROUTER_MODEL, OPENROUTER_MODELS,
    RAG_CHAT_MODELS,
    QWEN_EMBED_MODEL, QWEN_EMBED_API_URL,
    BASE_DIR,
)

provider_bp = Blueprint('provider', __name__)


@provider_bp.route("/api/provider/config")
def api_provider_config():
    provider = _provider_state["provider"]
    config = {
        "provider": provider,
        "autoAnalyze": _provider_state["auto_analyze"],
        "ollama": {
            "baseUrl": OLLAMA_BASE_URL,
            "chatModel": OLLAMA_CHAT_MODEL,
            "embedModel": OLLAMA_EMBED_MODEL,
            "thinking": OLLAMA_THINKING,
        },
        "anthropic": {
            "model": _config.LLM_MODEL,
            "available": bool(_config.LLM_API_KEY),
        },
        "openrouter": {
            "model": _provider_state.get("openrouter_model", OPENROUTER_MODEL),
            "models": [{"id": k, "label": v} for k, v in OPENROUTER_MODELS.items()],
            "available": bool(_config.OPENROUTER_API_KEY),
        },
        "ragChat": {
            "model": _provider_state.get("rag_chat_model", _config.RAG_CHAT_MODEL),
            "models": [{"id": k, "label": v} for k, v in RAG_CHAT_MODELS.items()],
        },
        "embedding": {
            "provider": _provider_state.get("embedding_provider", "ollama"),
            "providers": [
                {"id": "ollama", "label": "Ollama (Local, Free, Slow)", "info": "nomic-embed-text-v2-moe, ~1s/request"},
                {"id": "qwen", "label": "Qwen 3 Embedding 8B (Cloud, $0.0088/M, Fast)", "info": "0.88 РУБ/1M tokens, ~100ms/request"},
            ],
        },
    }
    if provider == "ollama":
        config["activeModel"] = OLLAMA_CHAT_MODEL
        config["activeCost"] = "$0.000"
        config["activeEstimate"] = "~5-10 сек" if not OLLAMA_THINKING else "~60 сек"
    elif provider == "openrouter":
        config["activeModel"] = _provider_state.get("openrouter_model", OPENROUTER_MODEL)
        config["activeCost"] = "$0.000"
        config["activeEstimate"] = "~5-15 сек"
    else:
        config["activeModel"] = _config.LLM_MODEL
        config["activeCost"] = "~$0.002"
        config["activeEstimate"] = "~2-3 сек"
    try:
        _env_text = (BASE_DIR / ".env").read_text(encoding="utf-8")
        import re as _re_saved
        _m_prov = _re_saved.search(r"^LLM_PROVIDER=(.+)$", _env_text, _re_saved.MULTILINE)
        _m_model = _re_saved.search(r"^OPENROUTER_MODEL=(.+)$", _env_text, _re_saved.MULTILINE)
        _sp = _m_prov.group(1).strip() if _m_prov else "ollama"
        _sm = _m_model.group(1).strip() if _m_model else OPENROUTER_MODEL
        config["savedDefault"] = _sp + ("/" + _sm if _sp == "openrouter" else "")
    except Exception:
        config["savedDefault"] = ""
    return jsonify(config)


@provider_bp.route("/api/provider/set", methods=["POST"])
def api_provider_set():
    data = request.get_json(silent=True) or {}
    provider = data.get("provider", "")
    if provider and provider not in ("ollama", "anthropic", "openrouter"):
        return jsonify({"error": "provider must be 'ollama', 'anthropic', or 'openrouter'"}), 400
    if provider:
        _provider_state["provider"] = provider
        print(f"[Provider] switched to {provider}")
    if "autoAnalyze" in data:
        _provider_state["auto_analyze"] = bool(data["autoAnalyze"])
        print(f"[Provider] auto_analyze={_provider_state['auto_analyze']}")
    if "openrouterModel" in data:
        model = data["openrouterModel"]
        if model in OPENROUTER_MODELS:
            _provider_state["openrouter_model"] = model
            print(f"[Provider] openrouter model switched to {model}")
    if "ragChatModel" in data:
        model = data["ragChatModel"]
        if model in RAG_CHAT_MODELS:
            _provider_state["rag_chat_model"] = model
            _config.RAG_CHAT_MODEL = model
            print(f"[Provider] RAG chat model switched to {model}")
    if "embeddingProvider" in data:
        provider = data["embeddingProvider"]
        if provider in ("ollama", "qwen"):
            _provider_state["embedding_provider"] = provider
            _config.EMBEDDING_PROVIDER = provider
            # No need to reimport — lazy dispatch in embeddings/__init__.py picks up change automatically
            print(f"[Provider] embedding provider switched to {provider}")
    _persist_provider_to_env()
    return jsonify({
        "ok": True,
        "provider": _provider_state["provider"],
        "autoAnalyze": _provider_state["auto_analyze"],
        "openrouterModel": _provider_state.get("openrouter_model", OPENROUTER_MODEL),
        "ragChatModel": _provider_state.get("rag_chat_model", _config.RAG_CHAT_MODEL),
        "embeddingProvider": _provider_state.get("embedding_provider", "ollama"),
    })
