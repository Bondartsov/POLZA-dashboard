#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polza.AI Dashboard v4 — Flask backend with PostgreSQL caching.
Refactored: modular package structure (config, providers, embeddings, workers, routes).
Запуск: python polza_dashboard.py [--port 5000] [--debug]
"""

import argparse
import os
import threading
from datetime import datetime, timezone

from config import (
    app, load_env, _provider_state, AUTH_TOKEN,
    LLM_API_URL, LLM_MODEL, LLM_API_KEY, LLM_PROVIDER,
    OLLAMA_BASE_URL, OLLAMA_CHAT_MODEL, OLLAMA_EMBED_MODEL,
    OLLAMA_THINKING, OLLAMA_TIMEOUT,
    OPENROUTER_API_KEY, OPENROUTER_MODEL, OPENROUTER_BASE_URL, OPENROUTER_MODELS,
    QDRANT_URL, QDRANT_COLLECTION, QDRANT_ENABLED, SYNC_INTERVAL,
    get_session, ApiKey, Generation, init_db, parse_keys_text, sync_worker,
)
from config import sync_worker as _sync_worker_module
from sync_worker import SyncWorker
from routes import register_all
from workers.analyze_all import _analyze_all, _analyze_all_worker
from embeddings.qdrant import _qdrant_ensure_collection


def main():
    global AUTH_TOKEN

    load_env()
    parser = argparse.ArgumentParser(description="Polza.AI Dashboard v4")
    parser.add_argument("--port", type=int, default=5000)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    import config
    config.AUTH_TOKEN = os.environ.get("POLZA_API_KEY", "")
    config.LLM_API_URL = os.environ.get("LLM_API_URL", config.LLM_API_URL)
    config.LLM_MODEL = os.environ.get("LLM_MODEL", config.LLM_MODEL)
    config.LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
    config.LLM_PROVIDER = os.environ.get("LLM_PROVIDER", config.LLM_PROVIDER)
    config.OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", config.OLLAMA_BASE_URL)
    config.OLLAMA_CHAT_MODEL = os.environ.get("OLLAMA_CHAT_MODEL", config.OLLAMA_CHAT_MODEL)
    config.OLLAMA_EMBED_MODEL = os.environ.get("OLLAMA_EMBED_MODEL", config.OLLAMA_EMBED_MODEL)
    config.OLLAMA_THINKING = os.environ.get("OLLAMA_THINKING", "").lower() in ("true", "1", "yes")
    config.OLLAMA_TIMEOUT = int(os.environ.get("OLLAMA_TIMEOUT", config.OLLAMA_TIMEOUT))
    config.OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
    config.OPENROUTER_MODEL = os.environ.get("OPENROUTER_MODEL", config.OPENROUTER_MODEL)
    config.OPENROUTER_BASE_URL = os.environ.get("OPENROUTER_BASE_URL", config.OPENROUTER_BASE_URL)
    config.QDRANT_URL = os.environ.get("QDRANT_URL", config.QDRANT_URL)
    config.QDRANT_COLLECTION = os.environ.get("QDRANT_COLLECTION", config.QDRANT_COLLECTION)
    config.QDRANT_ENABLED = os.environ.get("QDRANT_ENABLED", "true").lower() in ("true", "1", "yes")

    config.RAG_CHAT_MODEL = os.environ.get("RAG_CHAT_MODEL", config.RAG_CHAT_MODEL)
    config.RAG_MAX_SOURCES = int(os.environ.get("RAG_MAX_SOURCES", config.RAG_MAX_SOURCES))
    config.RAG_MIN_SCORE = float(os.environ.get("RAG_MIN_SCORE", config.RAG_MIN_SCORE))
    config.RAG_MAX_HISTORY = int(os.environ.get("RAG_MAX_HISTORY", config.RAG_MAX_HISTORY))
    config.RAG_MAX_TOKENS = int(os.environ.get("RAG_MAX_TOKENS", config.RAG_MAX_TOKENS))
    config.RAG_TEMPERATURE = float(os.environ.get("RAG_TEMPERATURE", config.RAG_TEMPERATURE))
    config.RAG_SESSION_TTL = int(os.environ.get("RAG_SESSION_TTL", config.RAG_SESSION_TTL))

    _provider_state["provider"] = config.LLM_PROVIDER
    _provider_state["openrouter_model"] = config.OPENROUTER_MODEL
    _provider_state["rag_chat_model"] = config.RAG_CHAT_MODEL
    auto_analyze_env = os.environ.get("AUTO_ANALYZE", "false").lower() in ("true", "1", "yes")
    _provider_state["auto_analyze"] = auto_analyze_env

    provider_icon = {"ollama": "On-Prem", "anthropic": "Cloud", "openrouter": "OpenRouter"}.get(config.LLM_PROVIDER, "?")
    print(f"LLM provider: {provider_icon} ({config.LLM_PROVIDER})")
    if config.LLM_PROVIDER == "ollama":
        print(f"   Ollama: {config.OLLAMA_BASE_URL}, model={config.OLLAMA_CHAT_MODEL}, embed={config.OLLAMA_EMBED_MODEL}")
    elif config.LLM_PROVIDER == "openrouter":
        print(f"   OpenRouter: model={config.OPENROUTER_MODEL}, key={'yes' if config.OPENROUTER_API_KEY else 'no'}")
    else:
        print(f"   Anthropic: url={config.LLM_API_URL}, model={config.LLM_MODEL}, key={'yes' if config.LLM_API_KEY else 'no'}")
    print(f"   Qdrant: {config.QDRANT_URL}/{config.QDRANT_COLLECTION} ({'enabled' if config.QDRANT_ENABLED else 'disabled'})")
    print(f"   Auto-analyze: {'ON' if auto_analyze_env else 'OFF'}")

    init_db()
    print("PostgreSQL connected")

    try:
        from config import get_analysis_state, get_analysis_counts
        prev = get_analysis_state()
        counts = get_analysis_counts()
        print(f"Analysis: {counts['analyzed']}/{counts['total']} analyzed, {counts['remaining']} remaining")
        if prev.status in ("running", "paused") and counts["remaining"] > 0:
            print(f"Resuming analyze-all (was {prev.status}, {counts['remaining']} remaining)")
            with _analyze_all["lock"]:
                _analyze_all["running"] = True
                _analyze_all["paused"] = prev.status == "paused"
                _analyze_all["done"] = 0
                _analyze_all["errors"] = prev.errors or 0
                _analyze_all["total"] = 0
                _analyze_all["stop_requested"] = False
                from config import update_analysis_state
                update_analysis_state(status="running")
                t = threading.Thread(target=_analyze_all_worker, daemon=True)
                _analyze_all["thread"] = t
                t.start()
    except Exception as e:
        print(f"Resume analyze-all failed: {e}")

    if config.QDRANT_ENABLED:
        if _qdrant_ensure_collection():
            print(f"Qdrant collection '{config.QDRANT_COLLECTION}' ready")
        else:
            print(f"Qdrant init failed — embeddings will be skipped")

    if config.AUTH_TOKEN:
        session = get_session()
        try:
            existing = session.query(ApiKey).filter(
                ApiKey.token == config.AUTH_TOKEN
            ).first()
            if not existing:
                pk = ApiKey(
                    name="Primary key", token=config.AUTH_TOKEN,
                    key_suffix=config.AUTH_TOKEN[-6:], is_primary=True
                )
                session.add(pk)
                session.commit()
                print(f"Primary key registered: ...{config.AUTH_TOKEN[-6:]}")
            else:
                print(f"Primary key exists: {existing.name}")
        finally:
            session.close()

    raw_keys = os.environ.get("POLZA_API_KEYS", "")
    if raw_keys:
        parsed = parse_keys_text(raw_keys)
        session = get_session()
        try:
            added = 0
            for k in parsed:
                existing = session.query(ApiKey).filter(
                    ApiKey.token == k["key"]
                ).first()
                if not existing:
                    ak = ApiKey(
                        name=k["name"], token=k["key"],
                        key_suffix=k["key"][-6:], is_primary=False
                    )
                    session.add(ak)
                    added += 1
            session.commit()
            if added:
                print(f"{added} additional keys from .env")
        finally:
            session.close()

    from workers.analyze_all import _auto_analyze_new_records
    sw = SyncWorker()
    sw.on_new_records = _auto_analyze_new_records
    sw.start()
    config.sync_worker = sw

    session = get_session()
    total_keys = session.query(ApiKey).count()
    total_gens = session.query(Generation).count()
    session.close()

    register_all(app)

    print(f"\nPolza.AI Dashboard v4")
    print(f"   {total_keys} keys  |  {total_gens} cached records")
    print(f"   Sync every {SYNC_INTERVAL // 60} min  |  http://{args.host}:{args.port}\n")

    app.run(host=args.host, port=args.port, debug=args.debug, threaded=True)


if __name__ == "__main__":
    main()
