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
from config import gen_summary_get_many, get_session, ApiKey, Generation
from rag.prompts import _build_context_block
from sqlalchemy import func as sa_func, desc as sa_desc

# Employee names cache — loaded once from DB
_employee_names = None
_employee_names_non_system = None


def _get_employee_names():
    """Load employee names from api_keys table. Cached for process lifetime."""
    global _employee_names, _employee_names_non_system
    if _employee_names is not None:
        return _employee_names

    _employee_names = []
    _employee_names_non_system = []
    _system_prefixes = ("AI-", "Основной", "Системный")
    try:
        session = get_session()
        try:
            keys = session.query(ApiKey.name).filter(
                ApiKey.name.isnot(None), ApiKey.name != ""
            ).all()
            _employee_names = [k[0] for k in keys]
            _employee_names_non_system = [
                k for k in _employee_names
                if not any(k.startswith(p) for p in _system_prefixes)
            ]
        finally:
            session.close()
    except Exception as e:
        print(f"[RAG][Search] employee names load failed: {e}")
        _employee_names = []
        _employee_names_non_system = []

    print(f"[RAG][Search] loaded {len(_employee_names)} employee names ({len(_employee_names_non_system)} non-system)")
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


_EMPLOYEE_LIST_KEYWORDS = [
    "фио", "фио всех", "список сотрудников", "кто делает запросы", "кто делает",
    "кто использует", "кто пользуется", "все сотрудники", "всех сотрудников",
    "имена", "кто запрос", "кто отправляет", "перечисли сотрудников",
    "список пользователей", "участники команды", "кто из команды",
    "сколько сотрудников", "какие сотрудники",
]

_GLOBAL_AGG_KEYWORDS = [
    "кто больше всего потратил", "кто потратил", "кто больше потратил",
    "больше всего потратил", "кто затратил", "кто израсходовал",
    "расход", "расходы", "затраты", "стоимость", "стоил",
    "потратил на ai", "потратили на", "кто сколько потратил",
    "кто использует самые", "общий расход", "общая стоимость",
    "статистик", "суммарн", "итого", "всего потрачено",
    "топ по расход", "рейтинг по расход", "кто самый активн",
    "кто чаще всего", "кто больше всего делает",
    "самые дорогие", "самые популярные модели",
    "кто использует gpt", "кто использует claude",
    "какие модели最受欢迎", "топ моделей",
    "сводка по команде", "обзор по команде", "аналитика по команде",
    "сколько запросов было", "сколько всего запросов",
    "статистика по всем", "по всем сотрудникам",
]


def _is_global_agg_query(query: str) -> bool:
    """Detect if the query requires global aggregation across ALL employees.
    
    These are questions about overall team stats, costs, rankings, etc.
    that cannot be answered by semantic search alone.
    """
    query_lower = query.lower()
    return any(kw in query_lower for kw in _GLOBAL_AGG_KEYWORDS)


def _is_employee_list_query(query: str) -> bool:
    """Detect if user asks for a list of all employees (not about a specific person)."""
    query_lower = query.lower()
    return any(kw in query_lower for kw in _EMPLOYEE_LIST_KEYWORDS)


def _build_global_aggregation() -> str:
    """Build comprehensive team-wide analytics context from PostgreSQL.
    
    Aggregates: per-employee costs, request counts, model usage, date ranges.
    This uses SQL queries directly against the generations table for accuracy.
    """
    global _employee_names_non_system
    if not _employee_names_non_system:
        _get_employee_names()
    
    names = _employee_names_non_system or []
    
    lines = [
        "=== ГЛОБАЛЬНАЯ АНАЛИТИКА ПО ВСЕЙ КОМАНДЕ ===",
        ""
    ]
    
    try:
        session = get_session()
        try:
            # 1) Per-employee aggregation: cost, count, tokens
            emp_stats = session.query(
                Generation.api_key_name,
                sa_func.count(Generation.id).label("total_requests"),
                sa_func.coalesce(sa_func.sum(Generation.cost), 0).label("total_cost"),
                sa_func.coalesce(sa_func.sum(Generation.total_tokens), 0).label("total_tokens"),
                sa_func.coalesce(sa_func.sum(Generation.prompt_tokens), 0).label("prompt_tokens"),
                sa_func.coalesce(sa_func.sum(Generation.completion_tokens), 0).label("completion_tokens"),
                sa_func.min(Generation.created_at_api).label("first_request"),
                sa_func.max(Generation.created_at_api).label("last_request"),
            ).filter(
                Generation.api_key_name.isnot(None),
                Generation.api_key_name != "",
            ).group_by(
                Generation.api_key_name
            ).order_by(
                sa_desc("total_cost")
            ).all()
            
            total_team_cost = sum(float(s.total_cost or 0) for s in emp_stats)
            total_team_requests = sum(int(s.total_requests or 0) for s in emp_stats)
            total_team_tokens = sum(int(s.total_tokens or 0) for s in emp_stats)
            
            lines.append(f"Всего записей в БД: {total_team_requests} запросов")
            lines.append(f"Общие расходы команды: ${total_team_cost:.4f}")
            lines.append(f"Общее количество токенов: {total_team_tokens:,}")
            lines.append("")
            
            # Filter to real employees only
            lines.append(f"РАСХОДЫ ПО СОТРУДНИКАМ ({len([s for s in emp_stats if s.api_key_name in names])} человек из команды):")
            lines.append("")
            
            for i, s in enumerate(emp_stats, 1):
                name = s.api_key_name or "Неизвестный"
                cost = float(s.total_cost or 0)
                req_count = int(s.total_requests or 0)
                tokens = int(s.total_tokens or 0)
                first = s.first_request.strftime("%Y-%m-%d") if s.first_request else "?"
                last = s.last_request.strftime("%Y-%m-%d") if s.last_request else "?"
                
                marker = "" if name in names else " [системный]"
                pct = (cost / total_team_cost * 100) if total_team_cost > 0 else 0
                
                lines.append(f"  {i}. {name}{marker}")
                lines.append(f"     Запросов: {req_count} | Стоимость: ${cost:.4f} ({pct:.1f}%) | Токенов: {tokens:,}")
                lines.append(f"     Период: {first} — {last}")
                lines.append("")
            
            # 2) Top models by usage
            model_stats = session.query(
                Generation.model_display_name,
                sa_func.count(Generation.id).label("total_requests"),
                sa_func.coalesce(sa_func.sum(Generation.cost), 0).label("total_cost"),
            ).filter(
                Generation.model_display_name.isnot(None),
            ).group_by(
                Generation.model_display_name
            ).order_by(
                sa_desc("total_cost")
            ).limit(15).all()
            
            if model_stats:
                lines.append("ТОП-15 МОДЕЛЕЙ ПО СТОИМОСТИ:")
                lines.append("")
                for i, m in enumerate(model_stats, 1):
                    model_name = m.model_display_name or "Unknown"
                    m_cost = float(m.total_cost or 0)
                    m_count = int(m.total_requests or 0)
                    pct = (m_cost / total_team_cost * 100) if total_team_cost > 0 else 0
                    lines.append(f"  {i}. {model_name} — {m_count} запросов, ${m_cost:.4f} ({pct:.1f}%)")
                lines.append("")
            
            # 3) Daily activity (last 14 days)
            from datetime import datetime, timezone, timedelta
            two_weeks_ago = datetime.now(timezone.utc) - timedelta(days=14)
            daily_stats = session.query(
                sa_func.date(Generation.created_at_api).label("day"),
                sa_func.count(Generation.id).label("cnt"),
                sa_func.coalesce(sa_func.sum(Generation.cost), 0).label("day_cost"),
            ).filter(
                Generation.created_at_api >= two_weeks_ago,
            ).group_by(
                sa_func.date(Generation.created_at_api)
            ).order_by(
                sa_func.date(Generation.created_at_api).desc()
            ).all()
            
            if daily_stats:
                lines.append("АКТИВНОСТЬ ПО ДНЯМ (последние 14 дней):")
                lines.append("")
                for d in daily_stats:
                    day_str = str(d.day) if d.day else "?"
                    day_cost = float(d.day_cost or 0)
                    day_cnt = int(d.cnt or 0)
                    lines.append(f"  {day_str}: {day_cnt} запросов, ${day_cost:.4f}")
                lines.append("")
            
            # 4) Non-work / suspicious activity
            try:
                from config import GenerationSummary
                suspicious = session.query(
                    GenerationSummary.is_work,
                    sa_func.count(GenerationSummary.generation_id).label("cnt"),
                ).filter(
                    GenerationSummary.is_work == False,  # noqa: E712
                ).group_by(
                    GenerationSummary.is_work
                ).first()
                
                non_work_count = int(suspicious.cnt) if suspicious else 0
                if non_work_count > 0:
                    lines.append(f"⚠️ ПОДОЗРИТЕЛЬНАЯ АКТИВНОСТЬ: {non_work_count} запросов с is_work=False (подозрение на личное использование)")
            except Exception:
                pass
            
        finally:
            session.close()
    except Exception as e:
        print(f"[RAG][Search] global aggregation error: {e}")
        lines.append(f"ОШИБКА агрегации: {e}")
    
    return "\n".join(lines)


def _build_employee_list_context() -> str:
    """Build context block listing all real employees with their stats from Qdrant.
    
    For each employee: name, total requests, date range, top models.
    """
    global _employee_names_non_system
    if not _employee_names_non_system:
        _get_employee_names()
    
    names = _employee_names_non_system or []
    if not names:
        return "ИСТОЧНИКИ: Список сотрудников недоступен."
    
    lines = [
        f"СПИСОК ВСЕХ СОТРУДНИКОВ, ДЕЛАЮЩИХ AI-ЗАПРОСЫ ({len(names)} человек):",
        ""
    ]
    
    # Try to get per-employee stats from Qdrant
    client = _get_qdrant_client()
    employee_stats = {}
    
    if client:
        try:
            _qdrant_ensure_collection()
            for name in sorted(names):
                try:
                    from qdrant_client.models import FieldCondition, Filter, MatchValue, CountResult
                    
                    emp_filter = Filter(
                        must=[FieldCondition(key="api_key_name", match=MatchValue(value=name))]
                    )
                    count_result = client.count(
                        collection_name=QDRANT_COLLECTION,
                        count_filter=emp_filter,
                    )
                    total = count_result.count
                    
                    # Get date range + last model via scroll (limit 1 for latest)
                    records, _ = client.scroll(
                        collection_name=QDRANT_COLLECTION,
                        scroll_filter=emp_filter,
                        limit=1,
                        with_payload=True,
                        with_vectors=False,
                    )
                    
                    last_date = ""
                    last_model = ""
                    if records:
                        p = records[0].payload
                        last_date = p.get("created_at", "")[:10]
                        last_model = p.get("model_display_name", "") or p.get("model_used", "")
                    
                    employee_stats[name] = {
                        "total": total,
                        "last_date": last_date,
                        "last_model": last_model,
                    }
                except Exception as e:
                    print(f"[RAG][Search] employee stat error for '{name}': {e}")
                    employee_stats[name] = {"total": 0, "last_date": "", "last_model": ""}
        except Exception as e:
            print(f"[RAG][Search] employee list stats error: {e}")
    
    for i, name in enumerate(sorted(names), 1):
        stats = employee_stats.get(name, {})
        total = stats.get("total", 0)
        if total > 0:
            lines.append(f"{i}. {name} — {total} запросов, последняя активность: {stats.get('last_date', '?')}, модель: {stats.get('last_model', '?')}")
        else:
            lines.append(f"{i}. {name} — запросов не найдено")
    
    return "\n".join(lines)


_DOSSIER_KEYWORDS = [
    "досье", "dossier", "всё", "все", "всего", "полный", "полная", "полностью",
    "история", "перечисли", "перечислить", "список", "сколько", "обзор",
    "сводка", "суммарно", "итого", "детальн", "подробн", "анализ",
    "что делал", "чем занимал", "какие запросы", "все запросы",
    "статистик", "активност", "расход",
]


def _is_dossier_query(query: str, employee_name: str or None) -> bool:
    """Detect if the query is a 'dossier mode' request — full employee activity dump.

    Triggered when: employee name detected + query contains aggregation/summary keywords,
    OR query explicitly asks about all activity of an employee.
    """
    if not employee_name:
        return False
    query_lower = query.lower()
    return any(kw in query_lower for kw in _DOSSIER_KEYWORDS)


def _qdrant_hybrid_search(query_vector: list, employee_name=None):
    """Dual search: semantic (always) + employee filter (if name detected). Merge + dedup."""
    client = _get_qdrant_client()
    if not client:
        return []

    _qdrant_ensure_collection()

    results = {}  # generation_id -> {payload, score}

    def _do_search(query_filter=None):
        """Run a single search using qdrant_client query_points API."""
        try:
            from qdrant_client.models import models as qmodels

            search_params = {
                "collection_name": QDRANT_COLLECTION,
                "query": query_vector,
                "limit": 30,
                "with_payload": True,
            }
            if query_filter:
                search_params["query_filter"] = query_filter

            response = client.query_points(**search_params)
            for r in response.points:
                gid = r.payload.get("generation_id", "")
                if gid:
                    results[gid] = {"payload": r.payload, "score": r.score}
        except Exception as e:
            print(f"[RAG][Search] search error: {e}")

    # A) Semantic search (always)
    _do_search()

    # B) Filtered search by employee name (if detected)
    if employee_name:
        try:
            from qdrant_client.models import FieldCondition, Filter, MatchValue

            emp_filter = Filter(
                must=[
                    FieldCondition(
                        key="api_key_name",
                        match=MatchValue(value=employee_name),
                    )
                ]
            )
            _do_search(query_filter=emp_filter)
        except Exception as e:
            print(f"[RAG][Search] filter build error: {e}")

    # Sort by score descending, take top RAG_MAX_SOURCES
    max_sources = getattr(config, "RAG_MAX_SOURCES", 20)
    min_score = getattr(config, "RAG_MIN_SCORE", 0.3)

    sorted_results = sorted(results.values(), key=lambda x: x["score"], reverse=True)
    sorted_results = [r for r in sorted_results if r["score"] >= min_score]
    sorted_results = sorted_results[:max_sources]

    return sorted_results


# START_BLOCK_DOSSIER_SCROLL
def _dossier_scroll(employee_name: str) -> list:
    """Dossier mode: scroll ALL vectors for a specific employee from Qdrant.

    Uses client.scroll() with filter to retrieve every record for the employee,
    bypassing the limit of semantic search. Returns list of {payload, score}.

    The score is set to 1.0 (exact filter match) since we're not doing semantic ranking.
    """
    client = _get_qdrant_client()
    if not client:
        return []

    _qdrant_ensure_collection()

    try:
        from qdrant_client.models import FieldCondition, Filter, MatchValue

        emp_filter = Filter(
            must=[
                FieldCondition(
                    key="api_key_name",
                    match=MatchValue(value=employee_name),
                )
            ]
        )

        all_records = []
        offset = None
        batch_size = 200

        while True:
            records, next_offset = client.scroll(
                collection_name=QDRANT_COLLECTION,
                scroll_filter=emp_filter,
                limit=batch_size,
                offset=offset,
                with_payload=True,
                with_vectors=False,
            )

            for r in records:
                gid = r.payload.get("generation_id", "")
                if gid:
                    all_records.append({
                        "payload": r.payload,
                        "score": 1.0,  # exact filter match, not semantic
                    })

            if next_offset is None or not records:
                break
            offset = next_offset

        print(f"[RAG][Search][DOSSIER] scrolled {len(all_records)} records for '{employee_name}'")
        return all_records

    except Exception as e:
        print(f"[RAG][Search][DOSSIER] scroll error: {e}")
        return []


def _build_dossier_aggregation(employee_name: str, records: list) -> str:
    """Build compact dossier summary from raw Qdrant records for LLM context.

    Instead of sending all 1000+ records to LLM, we aggregate:
    - total count
    - top topics (by frequency)
    - models used
    - date range
    - risk flags
    - work vs non-work ratio
    """
    from collections import Counter

    if not records:
        return ""

    topics = Counter()
    models = Counter()
    dates = []
    risk_count = 0
    non_work_count = 0
    projects = Counter()
    summaries_sample = []

    for r in records:
        p = r.get("payload", {})
        topic = p.get("topic", "")
        if topic:
            topics[topic] += 1
        model = p.get("model_display_name", "") or p.get("model_used", "")
        if model:
            models[model] += 1
        created = p.get("created_at", "")
        if created:
            dates.append(created[:10])  # YYYY-MM-DD
        is_work = p.get("is_work", True)
        if not is_work:
            non_work_count += 1
        project = p.get("project_guess", "")
        if project:
            projects[project] += 1
        # Collect summary snippets (for recent records)
        summary = p.get("summary", "")
        if summary and len(summaries_sample) < 30:
            date_str = created[:10] if created else "?"
            summaries_sample.append(f"  [{date_str}] {summary[:120]}")

    dates.sort()
    total = len(records)

    lines = [
        f"ДОСЬЕ СОТРУДНИКА: {employee_name}",
        f"Всего запросов: {total}",
        f"Период: {dates[0] if dates else '?'} — {dates[-1] if dates else '?'}",
        f"Рабочих: {total - non_work_count} | Подозрительных (не рабочих): {non_work_count}",
        "",
        f"ТОП-20 ТЕМ (из {len(topics)} уникальных):",
    ]
    for topic, count in topics.most_common(20):
        lines.append(f"  • {topic} — {count} запросов")

    if models:
        lines.append("")
        lines.append(f"МОДЕЛИ ({len(models)} уникальных):")
        for model, count in models.most_common(10):
            lines.append(f"  • {model} — {count} запросов")

    if projects:
        lines.append("")
        lines.append(f"ПРОЕКТЫ ({len(projects)} уникальных):")
        for proj, count in projects.most_common(10):
            lines.append(f"  • {proj} — {count} запросов")

    if non_work_count > 0:
        lines.append("")
        lines.append(f"⚠️ ПОДОЗРИТЕЛЬНАЯ АКТИВНОСТЬ: {non_work_count} запросов с is_work=False")

    # Add date distribution (last 14 days)
    if dates:
        date_counts = Counter(dates)
        lines.append("")
        lines.append("АКТИВНОСТЬ ПО ДНЯМ (последние 14 дней):")
        for d in sorted(date_counts.keys())[-14:]:
            lines.append(f"  {d}: {date_counts[d]} запросов")

    # Add sample summaries
    if summaries_sample:
        lines.append("")
        lines.append(f"ПРИМЕРЫ ЗАПРОСОВ (последние {len(summaries_sample)}):")
        for s in summaries_sample:
            lines.append(s)

    return "\n".join(lines)
# END_BLOCK_DOSSIER_SCROLL


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
            "topic": summary_data.get("topic", "") or payload.get("topic", ""),
            "summary": summary_data.get("summary", "") or payload.get("summary", ""),
            "is_work": summary_data.get("is_work", payload.get("is_work", True)) if summary_data else payload.get("is_work", True),
            "project_guess": summary_data.get("project_guess", "") or payload.get("project_guess", ""),
            "risk_flags": summary_data.get("risk_flags", []) or payload.get("risk_flags", []),
            "model": payload.get("model_display_name", "") or payload.get("model_used", ""),
            "session_id": payload.get("session_id", ""),
            "cost": payload.get("cost", 0),
            "client_cost": payload.get("client_cost", 0),
            "total_tokens": payload.get("total_tokens", 0),
            "prompt_tokens": payload.get("prompt_tokens", 0),
            "completion_tokens": payload.get("completion_tokens", 0),
            "provider": payload.get("provider", ""),
            "request_type": payload.get("request_type", ""),
            "finish_reason": payload.get("finish_reason", ""),
            "latency_ms": payload.get("latency_ms", 0),
            "generation_time_ms": payload.get("generation_time_ms", 0),
        })

    return enriched


# START_BLOCK_RAG_SEARCH
def _rag_search(query: str) -> dict:
    """Main RAG retrieval entry: embed query → detect mode (dossier vs semantic) → search → enrich → context block.

    Dossier mode: if employee name detected AND query has aggregation keywords,
    scroll ALL vectors for that employee (no limit, no min_score).
    Normal mode: hybrid semantic + employee filter search with score thresholds.

    Returns:
        {
            "sources": [...enriched source dicts...],
            "context_block": str,
            "count": int,
            "error": str or None,
            "mode": "dossier" or "search"
        }
    """
    if not query or not query.strip():
        return {"sources": [], "context_block": "", "count": 0, "error": "Empty query", "mode": "search"}

    query = query.strip()[:500]

    # Step 1: Embed query (always needed for context)
    try:
        query_vector = _embed_text(query)
    except Exception as e:
        print(f"[RAG][Search] embed failed: {e}")
        return {"sources": [], "context_block": "", "count": 0, "error": f"Embedding service unavailable: {e}", "mode": "search"}

    if not query_vector:
        return {"sources": [], "context_block": "", "count": 0, "error": "Embedding returned empty vector", "mode": "search"}

    # Step 2: Check if this is an "employee list" query (special mode)
    if _is_employee_list_query(query):
        print(f"[RAG][Search] employee list query detected")
        context_block = _build_employee_list_context()
        # Also run semantic search for supporting data
        search_results = _qdrant_hybrid_search(query_vector, None)
        enriched = _enrich_sources(search_results) if search_results else []
        
        return {
            "sources": enriched,
            "context_block": context_block,
            "count": len(enriched),
            "error": None,
            "mode": "employee_list"
        }

    # Step 3: Check if this is a global aggregation query (costs, stats across team)
    if _is_global_agg_query(query):
        print(f"[RAG][Search] global aggregation query detected")
        context_block = _build_global_aggregation()
        # Also run semantic search for supporting details
        search_results = _qdrant_hybrid_search(query_vector, None)
        enriched = _enrich_sources(search_results) if search_results else []
        
        return {
            "sources": enriched,
            "context_block": context_block,
            "count": len(enriched),
            "error": None,
            "mode": "global_agg"
        }

    # Step 4: Detect employee filter
    employee_name = _detect_employee_filter(query)

    # Step 4: Check if Dossier mode
    is_dossier = _is_dossier_query(query, employee_name)

    if is_dossier and employee_name:
        # DOSSIER MODE: scroll all records, build compact aggregation
        print(f"[RAG][Search][DOSSIER] activating dossier mode for '{employee_name}'")
        search_results = _dossier_scroll(employee_name)
        mode = "dossier"

        if not search_results:
            print(f"[RAG][Search][DOSSIER] no records for '{employee_name}'")
            return {
                "sources": [],
                "context_block": f"ИСТОЧНИКИ: Для сотрудника '{employee_name}' не найдено записей.",
                "count": 0,
                "error": None,
                "mode": mode
            }

        # Build compact aggregation instead of raw records
        dossier_agg = _build_dossier_aggregation(employee_name, search_results)
        total_count = len(search_results)

        # For UI: enrich only top 30 records (sample for source chips)
        sample_results = search_results[:30]
        enriched = _enrich_sources(sample_results)

        print(f"[RAG][Search][DOSSIER] '{employee_name}': {total_count} total, aggregation built, {len(enriched)} sample sources")

        return {
            "sources": enriched,
            "context_block": dossier_agg,
            "count": total_count,
            "error": None,
            "mode": mode
        }
    else:
        # NORMAL MODE: hybrid search
        search_results = _qdrant_hybrid_search(query_vector, employee_name)
        mode = "search"

    if not search_results:
        print(f"[RAG][Search] query='{query[:50]}' found=0 sources (mode={mode})")
        return {
            "sources": [],
            "context_block": "ИСТОЧНИКИ: По вашему запросу не найдено релевантных данных.",
            "count": 0,
            "error": None,
            "mode": mode
        }

    # Step 4: Enrich from PostgreSQL
    enriched = _enrich_sources(search_results)

    # Step 5: Build context block
    context_block = _build_context_block(enriched, mode)

    print(f"[RAG][Search] query='{query[:50]}' found={len(enriched)} sources mode={mode}"
          + (f" (filter: {employee_name})" if employee_name else ""))

    return {
        "sources": enriched,
        "context_block": context_block,
        "count": len(enriched),
        "error": None,
        "mode": mode
    }
# END_BLOCK_RAG_SEARCH
