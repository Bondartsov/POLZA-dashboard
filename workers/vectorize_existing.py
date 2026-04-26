"""
M-VECTORIZE-EXISTING: Background worker for vectorizing existing summaries.
Skips LLM analysis - only does embedding + Qdrant upsert for records that:
- Already have generation_summary in PostgreSQL
- Have is_vectorized = FALSE in DB
"""
import threading
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from config import (
    get_session, Generation, _resolve_token_for_gen, _headers,
    _provider_state, POLZA_API,
    GenerationSummary, gen_summary_mark_vectorized,
)
import requests as http_requests
from embeddings import _embed_text, _extract_user_text_from_log
from embeddings.qdrant import _qdrant_upsert
from embeddings.payload import _build_qdrant_payload


# START_MODULE_CONTRACT
# PURPOSE: Vectorize existing generation_summaries that lack Qdrant vectors
# SCOPE: Embedding-only worker — no LLM analysis, no summary creation
# INPUTS: triggered by POST /api/vectorize-existing/start
# OUTPUTS: Qdrant vectors + is_vectorized=TRUE in DB
# DEPENDS: embeddings dispatcher, Qdrant, GenerationSummary
# END_MODULE_CONTRACT


_vectorize_state = {
    "running": False,
    "stop_requested": False,
    "total": 0,
    "done": 0,
    "errors": 0,
    "skipped": 0,
    "thread": None,
    "started_at": None,
    "lock": threading.Lock(),
}


def get_vectorize_state() -> dict:
    """Return current state of vectorization job."""
    with _vectorize_state["lock"]:
        return {
            "running": _vectorize_state["running"],
            "total": _vectorize_state["total"],
            "done": _vectorize_state["done"],
            "errors": _vectorize_state["errors"],
            "skipped": _vectorize_state["skipped"],
            "startedAt": _vectorize_state["started_at"].isoformat() if _vectorize_state["started_at"] else None,
        }


def _vectorize_single_gen(gen_id: str) -> dict:
    """Vectorize a single existing summary (no LLM call)."""
    dbs = get_session()
    try:
        # Get summary and Generation meta
        summary = dbs.query(GenerationSummary).filter(
            GenerationSummary.generation_id == gen_id
        ).first()
        if not summary:
            return {"status": "skipped", "detail": "no summary in DB"}
        
        if summary.is_vectorized:
            return {"status": "skipped", "detail": "already vectorized"}
        
        gen_obj = dbs.query(Generation).get(gen_id)
        if not gen_obj:
            return {"status": "skipped", "detail": "no Generation record"}
        
        gen_meta = gen_obj.to_dict()
    finally:
        dbs.close()
    
    try:
        # Fetch log from Polza API to get text for embedding
        token = _resolve_token_for_gen(gen_id)
        r = http_requests.get(
            f"{POLZA_API}/history/generations/{gen_id}/log",
            headers=_headers(token), timeout=30,
        )
        if r.status_code != 200:
            return {"status": "skipped", "detail": f"HTTP {r.status_code}"}
        
        text = _extract_user_text_from_log(r.json(), limit_chars=4000)
        if not text:
            return {"status": "skipped", "detail": "empty text"}
        
        # Embedding only
        vector = _embed_text(text)
        if not vector:
            return {"status": "error", "detail": "embedding returned None"}
        
        # Build payload from existing summary + Generation meta
        payload = _build_qdrant_payload(
            gen_meta=gen_meta,
            analysis={
                "topic": summary.topic or "",
                "summary": summary.summary or "",
                "is_work": summary.is_work,
                "project_guess": summary.project_guess,
                "risk_flags": [],  # can be parsed from summary.risk_flags JSON
            },
            user_text_snippet=text,
        )
        
        # Upsert to Qdrant
        ok = _qdrant_upsert(gen_id, vector, payload)
        if not ok:
            return {"status": "error", "detail": "Qdrant upsert failed"}
        
        # Mark as vectorized in DB
        gen_summary_mark_vectorized(gen_id, True)
        return {"status": "ok"}
        
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def _vectorize_worker():
    """Worker thread: vectorize all unvectorized summaries."""
    print("[VectorizeExisting] worker started")
    try:
        # Find all unvectorized summaries
        dbs = get_session()
        try:
            unvec_ids = [
                row[0] for row in dbs.query(GenerationSummary.generation_id).filter(
                    GenerationSummary.is_vectorized == False
                ).all()
            ]
        finally:
            dbs.close()
        
        with _vectorize_state["lock"]:
            _vectorize_state["total"] = len(unvec_ids)
            _vectorize_state["done"] = 0
            _vectorize_state["errors"] = 0
            _vectorize_state["skipped"] = 0
        
        print(f"[VectorizeExisting] {len(unvec_ids)} records to vectorize")
        
        if not unvec_ids:
            print("[VectorizeExisting] nothing to do")
            return
        
        # Process with parallelism (3 workers)
        with ThreadPoolExecutor(max_workers=3) as pool:
            futures = []
            for gen_id in unvec_ids:
                if _vectorize_state["stop_requested"]:
                    print("[VectorizeExisting] stop requested")
                    break
                futures.append(pool.submit(_vectorize_single_gen, gen_id))
            
            for fut in futures:
                if _vectorize_state["stop_requested"]:
                    break
                try:
                    result = fut.result(timeout=60)
                    with _vectorize_state["lock"]:
                        if result["status"] == "ok":
                            _vectorize_state["done"] += 1
                        elif result["status"] == "skipped":
                            _vectorize_state["skipped"] += 1
                        else:
                            _vectorize_state["errors"] += 1
                except Exception as e:
                    print(f"[VectorizeExisting] worker error: {e}")
                    with _vectorize_state["lock"]:
                        _vectorize_state["errors"] += 1
                
                # Progress log every 50 records
                done = _vectorize_state["done"]
                if done > 0 and done % 50 == 0:
                    print(f"[VectorizeExisting] progress: {done}/{len(unvec_ids)} (errors={_vectorize_state['errors']})")
        
        print(f"[VectorizeExisting] completed: done={_vectorize_state['done']} errors={_vectorize_state['errors']} skipped={_vectorize_state['skipped']}")
    
    except Exception as e:
        print(f"[VectorizeExisting] fatal error: {e}")
    finally:
        with _vectorize_state["lock"]:
            _vectorize_state["running"] = False
            _vectorize_state["stop_requested"] = False


def start_vectorize_existing() -> dict:
    """Start vectorization in a background thread."""
    with _vectorize_state["lock"]:
        if _vectorize_state["running"]:
            return {"status": "already_running", "state": get_vectorize_state()}
        
        _vectorize_state["running"] = True
        _vectorize_state["stop_requested"] = False
        _vectorize_state["started_at"] = datetime.now(timezone.utc)
        
        t = threading.Thread(target=_vectorize_worker, daemon=True)
        _vectorize_state["thread"] = t
        t.start()
    
    return {"status": "started", "state": get_vectorize_state()}


def stop_vectorize_existing() -> dict:
    """Request stop of vectorization."""
    with _vectorize_state["lock"]:
        _vectorize_state["stop_requested"] = True
    return {"status": "stop_requested", "state": get_vectorize_state()}
