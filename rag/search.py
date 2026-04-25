# FILE: rag/search.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
#   PURPOSE: RAG retrieval pipeline — embed query, hybrid search Qdrant, enrich from PostgreSQL
#   SCOPE: Query embedding, employee name detection, Qdrant search with filters, DB enrichment
#   DEPENDS: M-CONFIG, M-EMBEDDING, M-QDRANT, M-GEN-SUMMARY-STORE
#   LINKS: M-RAG-SEARCH
# END_MODULE_CONTRACT

import json
import config
from embeddings.embed import _embed_text
from embeddings.qdrant import _get_qdrant_client, _qdrant_ensure_collection, QDRANT_COLLECTION
from config import gen_summary_get_many, get_session, ApiKey
from rag.prompts import _build_context_block

# Employee names cache — loaded once from DB
_employee_names = None


def _get_employee_names():
    """Load employee names from api_keys table. Cached for process lifetime."""
    global _employee_names
    if _employee_names is not None:
        return _employee_names

    _employee_names = []
    try:
        session = get_session()
        try:
            keys = session.query(ApiKey.name).filter(
                ApiKey.name.isnot(None), ApiKey.name != ""
            ).all()
            _employee_names = [k[0] for k in keys]
        finally:
            session.close()
    except Exception as e:
        print(f"[RAG][Search] employee names load failed: {e}")
        _employee_names = []

    print(f"[RAG][Search] loaded {len(_employee_names)} employee names")
    return _employee_names


def _detect_employee_filter(query: str):
    """Detect employee name in query text. Returns matching name or None."""
    names = _get_employee_names()
    query_lower = query.lower()

    # Sort by length descending to match longest name first (e.g. "Кузьмицкий Александр" > "Александр")
    for name in sorted(names, key=len, reverse=True):
        parts = name.split()
        # Check full name or any part (surname match is strongest)
        for part in parts:
            if len(part) >= 3 and part.lower() in query_lower:
                print(f"[RAG][Search] employee filter: {name} (matched '{part}')")
                return name
    return None


def _qdrant_hybrid_search(query_vector: list, employee_name=None):
    """Dual search: semantic (always) + employee filter (if name detected). Merge + dedup."""
    client = _get_qdrant_client()
    if not client:
        return []

    _qdrant_ensure_collection()

    results = {}  # generation_id -> {payload, score}

    # A) Semantic search (always)
    try:
        from qdrant_client.models import models as qmodels

        semantic = client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=query_vector,
            limit=30,
            with_payload=True,
        )
        for r in semantic:
            gid = r.payload.get("generation_id", "")
            if gid:
                results[gid] = {"payload": r.payload, "score": r.score}
    except Exception as e:
        print(f"[RAG][Search] semantic search error: {e}")

    # B) Filtered search by employee name (if detected)
    if employee_name:
        try:
            from qdrant_client.models import FieldCondition, Filter, MatchValue

            filtered = client.search(
                collection_name=QDRANT_COLLECTION,
                query_vector=query_vector,
                query_filter=Filter(
                    must=[
                        FieldCondition(
                            key="api_key_name",
                            match=MatchValue(value=employee_name),
                        )
                    ]
                ),
                limit=30,
                with_payload=True,
            )
            for r in filtered:
                gid = r.payload.get("generation_id", "")
                if gid:
                    # Keep higher score if already exists
                    if gid not in results or r.score > results[gid]["score"]:
                        results[gid] = {"payload": r.payload, "score": r.score}
        except Exception as e:
            print(f"[RAG][Search] filtered search error: {e}")

    # Sort by score descending, take top RAG_MAX_SOURCES
    max_sources = getattr(config, "RAG_MAX_SOURCES", 20)
    min_score = getattr(config, "RAG_MIN_SCORE", 0.3)

    sorted_results = sorted(results.values(), key=lambda x: x["score"], reverse=True)
    sorted_results = [r for r in sorted_results if r["score"] >= min_score]
    sorted_results = sorted_results[:max_sources]

    return sorted_results


def _enrich_sources(search_results: list) -> list:
    """Fetch full summaries from generation_summaries for matched IDs."""
    if not search_results:
        return []

    gen_ids = [r["payload"].get("generation_id") for r in search_results]
    gen_ids = [gid for gid in gen_ids if gid]

    # Batch fetch from PostgreSQL
    summaries = {}
    try:
        summaries = gen_summary_get_many(gen_ids)
    except Exception as e:
        print(f"[RAG][Search] enrichment error: {e}")

    enriched = []
    for result in search_results:
        payload = result.get("payload", {})
        gid = payload.get("generation_id", "")
        summary_data = summaries.get(gid, {})

        enriched.append({
            "generation_id": gid,
            "score": result["score"],
            "employee": payload.get("api_key_name", "Неизвестный"),
            "created_at": payload.get("created_at", ""),
            "topic": summary_data.get("topic", payload.get("topic", "")) if summary_data else payload.get("topic", ""),
            "summary": summary_data.get("summary", "") if summary_data else "",
            "is_work": summary_data.get("is_work", True) if summary_data else payload.get("is_work", True),
            "project_guess": summary_data.get("project_guess", "") if summary_data else "",
            "risk_flags": summary_data.get("risk_flags", []) if summary_data else [],
            "model": payload.get("model_used", ""),
            "session_id": payload.get("session_id", ""),
        })

    return enriched


# START_BLOCK_RAG_SEARCH
def _rag_search(query: str) -> dict:
    """Main RAG retrieval entry: embed query → hybrid search → enrich → context block.

    Returns:
        {
            "sources": [...enriched source dicts...],
            "context_block": str,
            "count": int,
            "error": str or None
        }
    """
    if not query or not query.strip():
        return {"sources": [], "context_block": "", "count": 0, "error": "Empty query"}

    query = query.strip()[:500]

    # Step 1: Embed query
    try:
        query_vector = _embed_text(query)
    except Exception as e:
        print(f"[RAG][Search] embed failed: {e}")
        return {"sources": [], "context_block": "", "count": 0, "error": f"Embedding service unavailable: {e}"}

    if not query_vector:
        return {"sources": [], "context_block": "", "count": 0, "error": "Embedding returned empty vector"}

    # Step 2: Detect employee filter
    employee_name = _detect_employee_filter(query)

    # Step 3: Hybrid search
    search_results = _qdrant_hybrid_search(query_vector, employee_name)

    if not search_results:
        print(f"[RAG][Search] query='{query[:50]}' found=0 sources")
        return {
            "sources": [],
            "context_block": "ИСТОЧНИКИ: По вашему запросу не найдено релевантных данных.",
            "count": 0,
            "error": None
        }

    # Step 4: Enrich from PostgreSQL
    enriched = _enrich_sources(search_results)

    # Step 5: Build context block
    context_block = _build_context_block(enriched)

    print(f"[RAG][Search] query='{query[:50]}' found={len(enriched)} sources"
          + (f" (filter: {employee_name})" if employee_name else ""))

    return {
        "sources": enriched,
        "context_block": context_block,
        "count": len(enriched),
        "error": None
    }
# END_BLOCK_RAG_SEARCH
