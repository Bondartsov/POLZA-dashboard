import requests as http_requests
from config import OLLAMA_BASE_URL, OLLAMA_CHAT_MODEL, OLLAMA_THINKING, OLLAMA_TIMEOUT
from providers.prompt import GEN_SUMMARIZE_PROMPT
from providers.anthropic import _parse_llm_json


def _llm_call_ollama(user_text: str):
    system_prompt = GEN_SUMMARIZE_PROMPT

    payload = {
        "model": OLLAMA_CHAT_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Запрос пользователя к AI-модели:\n\n{user_text}"},
        ],
        "stream": False,
        "think": OLLAMA_THINKING,
    }
    chat_url = f"{OLLAMA_BASE_URL}/api/chat"

    r = http_requests.post(chat_url, json=payload, timeout=OLLAMA_TIMEOUT)
    if r.status_code != 200:
        raise ValueError(f"Ollama HTTP {r.status_code}: {r.text[:500]}")

    data = r.json()
    content = ""
    msg = data.get("message", {})
    if msg:
        content = msg.get("content", "")

    if not content:
        raise ValueError("Ollama returned empty content")

    parsed = _parse_llm_json(content)

    usage_info = {
        "input_tokens": data.get("prompt_eval_count", 0) or 0,
        "output_tokens": data.get("eval_count", 0) or 0,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cost_usd": 0.0,
        "model": OLLAMA_CHAT_MODEL,
        "provider": "ollama",
    }
    return parsed, usage_info
