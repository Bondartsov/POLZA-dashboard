# Embedding provider dispatcher: select between Ollama (local) and Qwen (cloud)
from embeddings.qdrant import _get_qdrant_client, _qdrant_ensure_collection, _qdrant_upsert

def _get_embedding_functions():
    """Dynamically select embedding provider based on config."""
    import config
    
    provider = config._provider_state.get("embedding_provider", "ollama").lower()
    
    if provider == "qwen":
        from embeddings.qwen_embed import _embed_text_qwen, _extract_user_text_from_log_qwen
        
        # Wrap Qwen to match Ollama interface
        def _embed_text(text: str):
            """Embed via Qwen 3 Embedding 8B API."""
            return _embed_text_qwen(
                text,
                api_url=config.QWEN_EMBED_API_URL,
                api_key=config.AUTH_TOKEN,
                model=config.QWEN_EMBED_MODEL
            )
        
        _extract_user_text_from_log = _extract_user_text_from_log_qwen
        
    else:  # Default to Ollama
        from embeddings.embed import _embed_text as _embed_text_ollama
        from embeddings.embed import _extract_user_text_from_log as _extract_user_text_from_log_ollama
        
        _embed_text = _embed_text_ollama
        _extract_user_text_from_log = _extract_user_text_from_log_ollama
    
    return _embed_text, _extract_user_text_from_log

# Initialize with default provider
_embed_text, _extract_user_text_from_log = _get_embedding_functions()
