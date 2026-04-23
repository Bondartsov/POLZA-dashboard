import requests as http_requests
from config import OLLAMA_BASE_URL, OLLAMA_EMBED_MODEL


def _embed_text(text: str):
    if not text or not text.strip():
        return None
    try:
        r = http_requests.post(
            f"{OLLAMA_BASE_URL}/api/embed",
            json={"model": OLLAMA_EMBED_MODEL, "input": text[:2000]},
            timeout=30,
        )
        if r.status_code != 200:
            print(f"[Embed] Ollama HTTP {r.status_code}: {r.text[:200]}")
            return None
        data = r.json()
        embeddings = data.get("embeddings", [])
        if embeddings and len(embeddings[0]) == 768:
            return embeddings[0]
        print(f"[Embed] unexpected response shape: {len(embeddings)} vectors")
        return None
    except Exception as e:
        print(f"[Embed] error: {e}")
        return None


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
