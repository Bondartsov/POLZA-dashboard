import json
import re
import requests as http_requests
import config
from providers.prompt import GEN_SUMMARIZE_PROMPT


def _parse_llm_json(raw_text: str) -> dict:
    json_str = raw_text.strip()
    json_str = re.sub(r"<think[^>]*/\s*>", "", json_str)
    json_str = re.sub(r"<think[^>]*>.*?</think\s*>", "", json_str, flags=re.DOTALL)
    json_str = json_str.strip()
    try:
        return json.loads(json_str)
    except json.JSONDecodeError:
        pass
    m = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", json_str, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(1).strip())
        except json.JSONDecodeError:
            pass
    m = re.search(r"\{[^{}]*\}", json_str, re.DOTALL)
    if m:
        try:
            return json.loads(m.group(0))
        except json.JSONDecodeError:
            pass
    depth = 0
    start = None
    for i, ch in enumerate(json_str):
        if ch == '{':
            if depth == 0:
                start = i
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0 and start is not None:
                candidate = json_str[start:i + 1]
                candidate = re.sub(r",\s*([}\]])", r"\1", candidate)
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    start = None
    raise json.JSONDecodeError("Could not extract JSON from LLM output", json_str, 0)


def _llm_call_anthropic(user_text: str):
    llm_payload = {
        "model": config.LLM_MODEL,
        "max_tokens": 900,
        "temperature": 0.2,
        "system": [
            {
                "type": "text",
                "text": GEN_SUMMARIZE_PROMPT,
                "cache_control": {"type": "ephemeral"},
            },
        ],
        "messages": [
            {"role": "user", "content": f"Запрос пользователя к AI-модели:\n\n{user_text}"},
        ],
    }
    _llm_headers = {
        "x-api-key": config.LLM_API_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json",
    }
    llm_r = http_requests.post(
        config.LLM_API_URL, headers=_llm_headers, json=llm_payload, timeout=45,
    )
    if llm_r.status_code != 200:
        raise ValueError(f"Anthropic HTTP {llm_r.status_code}: {llm_r.text[:300]}")

    llm_response = llm_r.json()
    llm_content = ""
    for block in llm_response.get("content", []):
        if block.get("type") == "text":
            llm_content += block.get("text", "")

    if not llm_content:
        raise ValueError("Anthropic returned empty content")

    parsed = _parse_llm_json(llm_content)

    usage = llm_response.get("usage", {}) or {}
    usage_info = {
        "input_tokens": usage.get("input_tokens", 0),
        "output_tokens": usage.get("output_tokens", 0),
        "cache_creation_input_tokens": usage.get("cache_creation_input_tokens", 0),
        "cache_read_input_tokens": usage.get("cache_read_input_tokens", 0),
    }
    cost = (
        usage_info["input_tokens"] * 1.0
        + usage_info["output_tokens"] * 5.0
        + usage_info["cache_creation_input_tokens"] * 1.25
        + usage_info["cache_read_input_tokens"] * 0.10
    ) / 1_000_000
    usage_info["cost_usd"] = round(cost, 6)
    usage_info["model"] = config.LLM_MODEL
    usage_info["provider"] = "anthropic"
    return parsed, usage_info
