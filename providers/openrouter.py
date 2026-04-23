import time
import requests as http_requests
import config
from config import _provider_state, OPENROUTER_MODEL
from providers.prompt import GEN_SUMMARIZE_PROMPT
from providers.anthropic import _parse_llm_json


def _llm_call_openrouter(user_text: str):
    model = _provider_state.get("openrouter_model", OPENROUTER_MODEL)
    payload = {
        "model": model,
        "max_tokens": 900,
        "temperature": 0.2,
        "reasoning": {"enabled": False},
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": GEN_SUMMARIZE_PROMPT},
            {"role": "user", "content": f"Запрос пользователя к AI-модели:\n\n{user_text}"},
        ],
    }
    headers = {
        "Authorization": f"Bearer {config.OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://polza-dashboard.local",
        "X-Title": "Polza.AI Dashboard",
    }
    url = f"{config.OPENROUTER_BASE_URL}/chat/completions"

    max_retries = 4
    for attempt in range(max_retries + 1):
        r = http_requests.post(url, headers=headers, json=payload, timeout=120)
        if r.status_code == 200:
            break
        if r.status_code == 429 and attempt < max_retries:
            wait = 2 ** (attempt + 1)
            print(f"[Provider][OpenRouter] 429 rate-limited, retry {attempt+1}/{max_retries} in {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code != 200:
            raise ValueError(f"OpenRouter HTTP {r.status_code}: {r.text[:500]}")

    data = r.json()
    content = ""
    choices = data.get("choices", [])
    if choices:
        content = choices[0].get("message", {}).get("content", "")

    if not content:
        raise ValueError("OpenRouter returned empty content")

    print(f"[Provider][OpenRouter] raw response ({len(content)} chars): {content[:300]}")
    parsed = _parse_llm_json(content)

    or_usage = data.get("usage", {}) or {}
    usage_info = {
        "input_tokens": or_usage.get("prompt_tokens", 0),
        "output_tokens": or_usage.get("completion_tokens", 0),
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "cost_usd": 0.0,
        "model": model,
        "provider": "openrouter",
    }
    return parsed, usage_info
