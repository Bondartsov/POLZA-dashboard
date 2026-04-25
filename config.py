import argparse, json, os, sys, threading, time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from datetime import datetime, timezone, timedelta
from flask import Flask, jsonify, request, send_from_directory
import requests as http_requests
import re
from sqlalchemy import func, desc, asc, String as SaString

if sys.platform == "win32":
    os.system("")
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"
POLZA_API = "https://polza.ai/api/v1"
SYNC_INTERVAL = 300

from db import init_db, get_session, ApiKey, Generation, engine
from db import SessionSummary, summary_get_or_none, summary_upsert
from db import (
    GenerationSummary,
    gen_summary_get_or_none,
    gen_summary_get_many,
    gen_summary_upsert,
    gen_summary_delete,
)
from db import AnalysisState, get_analysis_state, update_analysis_state, get_analysis_counts
from sync_worker import SyncWorker, sync_all_keys

app = Flask(__name__, static_folder=str(STATIC_DIR), static_url_path="")
sync_worker = None
AUTH_TOKEN = ""

LLM_API_URL = "https://api.anthropic.com/v1/messages"
LLM_MODEL = "claude-haiku-4-5"
LLM_API_KEY = ""

LLM_PROVIDER = "ollama"
OLLAMA_BASE_URL = "http://localhost:11434"
OLLAMA_CHAT_MODEL = "qwen3.5:4b"
OLLAMA_EMBED_MODEL = "nomic-embed-text-v2-moe:latest"
OLLAMA_THINKING = False
OLLAMA_TIMEOUT = 120

OPENROUTER_API_KEY = ""
OPENROUTER_MODEL = "nvidia/nemotron-3-super-120b-a12b:free"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"

OPENROUTER_MODELS = {
    "nvidia/nemotron-3-super-120b-a12b:free": "Nemotron 3 Super 120B",
    "google/gemma-4-31b-it:free": "Gemma 4 31B",
}

QDRANT_URL = "http://localhost:6335"
QDRANT_COLLECTION = "Polza_user_logs"
QDRANT_ENABLED = True

RAG_CHAT_MODEL = "nvidia/nemotron-3-nano-30b-a3b:free"
RAG_MAX_SOURCES = 20
RAG_MIN_SCORE = 0.3
RAG_MAX_HISTORY = 20
RAG_MAX_TOKENS = 2000
RAG_TEMPERATURE = 0.3
RAG_SESSION_TTL = 7200

_provider_state = {"provider": "ollama", "auto_analyze": False, "openrouter_model": OPENROUTER_MODEL}


def load_env():
    p = BASE_DIR / ".env"
    if not p.exists():
        return
    raw = p.read_text(encoding="utf-8-sig")
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue
        k, _, v = line.partition("=")
        k = k.strip()
        if not k or k in os.environ:
            continue
        v = v.strip()
        if v.startswith('"'):
            end = v.find('"', 1)
            v = v[1:end] if end > 0 else v[1:]
        elif v.startswith("'"):
            end = v.find("'", 1)
            v = v[1:end] if end > 0 else v[1:]
        else:
            ci = v.find(" #")
            if ci > 0:
                v = v[:ci]
            v = v.strip().strip("\"'")
        os.environ[k] = v


def _persist_provider_to_env():
    env_path = BASE_DIR / ".env"
    if not env_path.exists():
        return
    try:
        text = env_path.read_text(encoding="utf-8")
        provider = _provider_state["provider"]
        model = _provider_state.get("openrouter_model", OPENROUTER_MODEL)
        import re as _re
        if "LLM_PROVIDER=" in text:
            text = _re.sub(r"^LLM_PROVIDER=.*$", f"LLM_PROVIDER={provider}", text, flags=re.MULTILINE)
        else:
            text += f"\nLLM_PROVIDER={provider}\n"
        if "OPENROUTER_MODEL=" in text:
            text = _re.sub(r"^OPENROUTER_MODEL=.*$", f"OPENROUTER_MODEL={model}", text, flags=re.MULTILINE)
        else:
            text += f"\nOPENROUTER_MODEL={model}\n"
        env_path.write_text(text, encoding="utf-8")
    except Exception as e:
        print(f"[Provider] persist to .env failed: {e}")


def parse_keys_text(text):
    keys = []
    for line in text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        name, key = "", ""
        if "\t" in line:
            parts = line.split("\t")
            idx = next((i for i, p in enumerate(parts)
                        if p.strip().startswith("pza_") or p.strip().startswith("sk-")), -1)
            if idx >= 0:
                key = parts[idx].strip()
                name = " ".join(p.strip() for p in parts[:idx]) or key[-6:]
            else:
                name = parts[0].strip()
                key = parts[-1].strip()
        elif "pza_" in line:
            idx = line.index("pza_")
            name = line[:idx].strip()
            key = line[idx:].strip()
        elif "sk-" in line:
            idx = line.index("sk-")
            name = line[:idx].strip()
            key = line[idx:].strip()
        else:
            continue
        if key and (key.startswith("pza_") or key.startswith("sk-")):
            keys.append({"key": key, "name": name or key[-6:]})
    return keys


def _resolve_token_for_gen(gen_id):
    session = get_session()
    try:
        gen = session.query(Generation).get(gen_id)
        if gen and gen.source_key_name:
            key = session.query(ApiKey).filter(
                ApiKey.name == gen.source_key_name
            ).first()
            if key:
                return key.token
        return AUTH_TOKEN
    finally:
        session.close()


def _headers(token=None):
    return {"Authorization": f"Bearer {token or AUTH_TOKEN}", "Accept": "application/json"}
