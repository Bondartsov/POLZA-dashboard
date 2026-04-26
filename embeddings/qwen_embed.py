"""
Qwen 3 Embedding 8B integration via Polza API.
Cost: 0.88 RUB/1M tokens (~$0.0088/1M)
Speed: ~1000 req/sec (vs 1 req/sec on Ollama local)
"""
import requests
from typing import List, Optional
import logging

logger = logging.getLogger(__name__)

# START_MODULE_CONTRACT
# PURPOSE: Cloud-based embeddings via Qwen 3 Embedding 8B through Polza API
# SCOPE: Text to 768-dim vector conversion via HTTP API
# INPUTS: text (str), api_url (str), api_key (str)
# OUTPUTS: 768-dimensional embedding vector (List[float])
# DEPENDS: requests, config (QWEN_EMBED_API_URL, AUTH_TOKEN)
# END_MODULE_CONTRACT


def _embed_text_qwen(
    text: str,
    api_url: str,
    api_key: str,
    model: str = "qwen/qwen3-embedding-8b",
    timeout: int = 30
) -> Optional[List[float]]:
    """
    Embed text via Qwen 3 Embedding 8B API.
    
    Args:
        text: Text to embed (up to 8000 tokens)
        api_url: Base API URL (e.g., https://polza.ai/api/v1)
        api_key: Polza API key (same as for generation API)
        model: Model identifier (default: qwen/qwen3-embedding-8b)
        timeout: Request timeout in seconds
    
    Returns:
        List of 768 floats (embedding vector) or None on error
        
    Cost: ~0.0088 USD per 1M tokens
    Speed: ~100-200ms per request
    """
    
    if not text or not text.strip():
        logger.warning("Empty text for embedding")
        return None
    
    try:
        # START_BLOCK_QWEN_EMBED_REQUEST
        url = f"{api_url}/embeddings"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": model,
            "input": text.strip(),
            "encoding_format": "float",
            "dimensions": 768  # Matryoshka truncation — match Qdrant collection size
        }
        
        response = requests.post(url, json=payload, headers=headers, timeout=timeout)
        response.raise_for_status()
        # END_BLOCK_QWEN_EMBED_REQUEST
        
        # START_BLOCK_QWEN_EMBED_PARSE
        data = response.json()
        
        # Handle OpenAI-compatible response format
        if "data" in data and len(data["data"]) > 0:
            embedding = data["data"][0].get("embedding")
            if embedding:
                return embedding
        
        # Handle direct embedding in response
        if "embedding" in data:
            return data["embedding"]
        
        logger.error(f"Unexpected response format from Qwen API: {data}")
        return None
        # END_BLOCK_QWEN_EMBED_PARSE
        
    except requests.exceptions.Timeout:
        logger.error(f"Qwen embedding request timeout ({timeout}s)")
        return None
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Qwen API connection error: {e}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.error(f"Qwen API HTTP error: {e.response.status_code} - {e.response.text}")
        return None
    except Exception as e:
        logger.error(f"Qwen embedding error: {e}", exc_info=True)
        return None


def _extract_user_text_from_log_qwen(log_dict: dict, limit_chars: int = 4000) -> str:
    """
    Extract and concatenate user-role messages from Polza log.
    Same as for Ollama embedding (compatibility).
    
    Args:
        log_dict: Polza log structure
        limit_chars: Maximum characters to include
        
    Returns:
        Concatenated user text (capped at limit_chars)
    """
    if not log_dict or not isinstance(log_dict, dict):
        return ""
    
    messages = log_dict.get("messages", [])
    if not messages:
        return ""
    
    user_texts = []
    for msg in messages:
        role = msg.get("role", "").lower()
        content = msg.get("content", "")
        
        if role == "user" and content:
            user_texts.append(content)
    
    result = "\n".join(user_texts)
    if len(result) > limit_chars:
        result = result[:limit_chars]
    
    return result
