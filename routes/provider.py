from flask import Blueprint, jsonify, request
import config as _config
from config import (
    _provider_state, _persist_provider_to_env,
    OLLAMA_BASE_URL, OLLAMA_CHAT_MODEL, OLLAMA_EMBED_MODEL, OLLAMA_THINKING,
    OPENROUTER_MODEL, OPENROUTER_MODELS,
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
        _provider_state["auto_analyze"] = bool(data["auto_analyze"])
        print(f"[Provider] auto_analyze={_provider_state['auto_analyze']}")
    if "openrouterModel" in data:
        model = data["openrouterModel"]
        if model in OPENROUTER_MODELS:
            _provider_state["openrouter_model"] = model
            print(f"[Provider] openrouter model switched to {model}")

    if data.get("saveDefault"):
        _persist_provider_to_env()
        print("[Provider] Persisted settings to .env")

    return jsonify({
        "ok": True,
        "provider": _provider_state["provider"],
        "autoAnalyze": _provider_state["auto_analyze"],
        "openrouterModel": _provider_state.get("openrouter_model", OPENROUTER_MODEL),
    })
