import threading
import time
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

from config import (
    get_session, Generation, _resolve_token_for_gen, _headers,
    _provider_state, OLLAMA_TIMEOUT, POLZA_API,
    gen_summary_get_or_none, gen_summary_get_many, gen_summary_upsert,
    get_analysis_state, update_analysis_state, get_analysis_counts,
)
import requests as http_requests
from providers.dispatcher import _llm_call_summarize
from embeddings import _embed_text, _extract_user_text_from_log
from embeddings.qdrant import _qdrant_upsert
from embeddings.payload import _build_qdrant_payload

_analyze_all = {
    "running": False,
    "paused": False,
    "total": 0,
    "done": 0,
    "errors": 0,
    "skipped": 0,
    "stop_requested": False,
    "thread": None,
    "lock": threading.Lock(),
}


def _analyze_single_gen(gen_id: str) -> dict:
    cached = gen_summary_get_or_none(gen_id)
    if cached:
        return {"status": "skipped"}

    try:
        token = _resolve_token_for_gen(gen_id)
        r = http_requests.get(
            f"{POLZA_API}/history/generations/{gen_id}/log",
            headers=_headers(token), timeout=30,
        )
        if r.status_code != 200:
            return {"status": "error", "detail": f"HTTP {r.status_code}"}

        total_text = _extract_user_text_from_log(r.json(), limit_chars=4000)
        if not total_text:
            return {"status": "skipped", "detail": "empty text"}

        llm_result = [None, None]
        embed_result = [None]

        def _run_llm():
            llm_result[0], llm_result[1] = _llm_call_summarize(total_text)

        def _run_embed():
            embed_result[0] = _embed_text(total_text)

        with ThreadPoolExecutor(max_workers=2) as pool:
            llm_future = pool.submit(_run_llm)
            embed_future = pool.submit(_run_embed)
            llm_future.result(timeout=OLLAMA_TIMEOUT if _provider_state["provider"] == "ollama" else 45)
            embed_future.result(timeout=30)

        parsed, usage = llm_result

        try:
            gen_summary_upsert(
                generation_id=gen_id,
                summary=parsed.get("summary", ""),
                topic=parsed.get("topic", ""),
                is_work=parsed.get("is_work", True),
                project_guess=parsed.get("project_guess"),
                risk_flags=parsed.get("risk_flags", []),
                llm_model=usage.get("model", ""),
                llm_cost=usage["cost_usd"],
                cache_creation_tokens=usage.get("cache_creation_input_tokens", 0),
                cache_read_tokens=usage.get("cache_read_input_tokens", 0),
                input_tokens=usage["input_tokens"],
                output_tokens=usage["output_tokens"],
            )
        except Exception as e:
            print(f"[AnalyzeAll][cache_store] non-fatal: {e}")

        if embed_result[0]:
            dbs = get_session()
            gen_meta = None
            try:
                gen_obj = dbs.query(Generation).get(gen_id)
                if gen_obj:
                    gen_meta = gen_obj.to_dict()
            finally:
                dbs.close()
            qdrant_payload = _build_qdrant_payload(
                gen_meta=gen_meta or {},
                analysis=parsed,
                user_text_snippet=total_text,
            )
            _qdrant_upsert(gen_id, embed_result[0], qdrant_payload)

        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}


def _analyze_all_worker():
    print(f"[AnalyzeAll] worker started")
    try:
        while True:
            with _analyze_all["lock"]:
                if _analyze_all["stop_requested"]:
                    update_analysis_state(status="paused")
                    _analyze_all["running"] = False
                    print("[AnalyzeAll] stopped by request")
                    return

            dbs = get_session()
            try:
                all_gen_ids = dbs.query(Generation.id).order_by(Generation.created_at_api.desc()).all()
                all_ids = [row[0] for row in all_gen_ids]

                cached_ids = set()
                for i in range(0, len(all_ids), 500):
                    batch = all_ids[i:i + 500]
                    found = gen_summary_get_many(batch)
                    cached_ids.update(found.keys())

                uncached = [gid for gid in all_ids if gid not in cached_ids]

                with _analyze_all["lock"]:
                    _analyze_all["total"] = len(uncached)

                update_analysis_state(
                    status="running",
                    total=len(all_ids),
                    done=len(cached_ids),
                    skipped=0,
                    errors=_analyze_all["errors"],
                )

                if not uncached:
                    update_analysis_state(
                        status="completed",
                        total=len(all_ids),
                        done=len(cached_ids),
                    )
                    with _analyze_all["lock"]:
                        _analyze_all["running"] = False
                    print(f"[AnalyzeAll] done — all {len(all_ids)} generations analyzed")
                    return

                print(f"[AnalyzeAll] {len(uncached)} remaining (of {len(all_ids)} total)")

                for gen_id in uncached:
                    with _analyze_all["lock"]:
                        if _analyze_all["stop_requested"]:
                            update_analysis_state(status="paused")
                            _analyze_all["running"] = False
                            print("[AnalyzeAll] stopped mid-batch")
                            return
                        while _analyze_all["paused"] and not _analyze_all["stop_requested"]:
                            _analyze_all["lock"].release()
                            update_analysis_state(status="paused")
                            time.sleep(0.5)
                            _analyze_all["lock"].acquire()
                        if _analyze_all["stop_requested"]:
                            update_analysis_state(status="paused")
                            _analyze_all["running"] = False
                            return

                    result = _analyze_single_gen(gen_id)

                    with _analyze_all["lock"]:
                        if result["status"] == "ok":
                            _analyze_all["done"] += 1
                        elif result["status"] == "skipped":
                            _analyze_all["skipped"] += 1
                        else:
                            _analyze_all["errors"] += 1

                    local_done = _analyze_all["done"]
                    if local_done % 5 == 0:
                        update_analysis_state(
                            status="running",
                            total=len(all_ids),
                            done=local_done,
                            skipped=_analyze_all["skipped"],
                            errors=_analyze_all["errors"],
                        )

                    delay = 1.0 if _provider_state.get("provider") in ("openrouter", "anthropic") else 0.3
                    time.sleep(delay)

            except Exception as e:
                update_analysis_state(status="error")
                with _analyze_all["lock"]:
                    _analyze_all["running"] = False
                print(f"[AnalyzeAll] fatal error: {e}")
                return
            finally:
                dbs.close()
            break

        update_analysis_state(
            status="completed",
            total=_analyze_all["total"] + _analyze_all["done"],
            done=_analyze_all["done"],
            skipped=_analyze_all["skipped"],
            errors=_analyze_all["errors"],
            completed_at=datetime.now(timezone.utc),
        )
        with _analyze_all["lock"]:
            _analyze_all["running"] = False
        print(f"[AnalyzeAll] completed: done={_analyze_all['done']} skipped={_analyze_all['skipped']} errors={_analyze_all['errors']}")
    except Exception as e:
        print(f"[AnalyzeAll] unexpected error: {e}")
        update_analysis_state(status="error")
        with _analyze_all["lock"]:
            _analyze_all["running"] = False


def _auto_analyze_new_records(new_gen_ids):
    if not new_gen_ids:
        return
    if not _provider_state.get("auto_analyze"):
        return
    with _analyze_all["lock"]:
        if _analyze_all["running"]:
            print(f"[AutoAnalyze] skipped — analyze-all already running")
            return

    cached = gen_summary_get_many(new_gen_ids)
    uncached = [gid for gid in new_gen_ids if gid not in cached]

    if not uncached:
        return

    print(f"[AutoAnalyze] {len(uncached)} new records to auto-analyze (provider={_provider_state['provider']})")

    for gen_id in uncached:
        try:
            result = _analyze_single_gen(gen_id)
            if result["status"] == "ok":
                print(f"[AutoAnalyze] ok {gen_id[:16]}")
            else:
                print(f"[AutoAnalyze] warn {gen_id[:16]}: {result.get('detail', result['status'])}")
        except Exception as e:
            print(f"[AutoAnalyze] err {gen_id[:16]}: {e}")
        time.sleep(0.5)

    print(f"[AutoAnalyze] batch done")
