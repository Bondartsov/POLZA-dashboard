# Embedding provider dispatcher: lazy selection between Ollama (local) and Qwen (cloud)
import os
from embeddings.qdrant import _get_qdrant_client, _qdrant_ensure_collection, _qdrant_upsert


def _embed_text(text: str):
    """Embed text using current provider (lazy dispatch).
    
    Checks _provider_state.embedding_provider on every call,
    so runtime switching via /api/provider/set works immediately.
    """
    import config
    provider = config._provider_state.get("embedding_provider", "ollama").lower()
    
    if provider == "qwen":
        from embeddings.qwen_embed import _embed_text_qwen
        api_key = os.environ.get("POLZA_API_KEY", config.QWEN_EMBED_API_KEY or "")
        if not api_key:
            print("[Embedding][Qwen] ERROR: No POLZA_API_KEY found in environment")
            return None
        return _embed_text_qwen(
            text,
            api_url=config.QWEN_EMBED_API_URL,
            api_key=api_key,
            model=config.QWEN_EMBED_MODEL
        )
    else:
        from embeddings.embed import _embed_text as _embed_ollama
        return _embed_ollama(text)


def _extract_user_text_from_log(log_dict: dict, max_chars: int = 4000) -> str:
    """Extract user text from log (same logic for both providers)."""
    from embeddings.embed import _extract_user_text_from_log as _extract
    return _extract(log_dict, max_chars)
