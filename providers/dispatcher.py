from config import _provider_state
from providers.anthropic import _llm_call_anthropic
from providers.ollama import _llm_call_ollama
from providers.openrouter import _llm_call_openrouter


def _llm_call_summarize(user_text: str):
    provider = _provider_state["provider"]
    if provider == "ollama":
        return _llm_call_ollama(user_text)
    elif provider == "openrouter":
        return _llm_call_openrouter(user_text)
    else:
        return _llm_call_anthropic(user_text)
