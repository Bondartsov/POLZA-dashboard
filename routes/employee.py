from collections import Counter
from datetime import datetime, timezone, timedelta
from flask import Blueprint, jsonify, request
from sqlalchemy import func, desc

from config import get_session, Generation, summary_get_or_none

employee_bp = Blueprint('employee', __name__)


# ─── START_BLOCK_ANOMALY

def detect_anomalies(generations_list):
    anomalies = []
    if not generations_list:
        return anomalies

    off_hours = [g for g in generations_list
                 if g.created_at_api and (g.created_at_api.hour >= 22 or g.created_at_api.hour < 8)]
    if off_hours:
        sev = "high" if len(off_hours) > 10 else "medium" if len(off_hours) > 3 else "low"
        anomalies.append({
            "type": "OFF_HOURS", "severity": sev, "count": len(off_hours),
            "details": f"{len(off_hours)} запросов в нерабочее время (22:00–08:00)",
        })

    weekend = [g for g in generations_list
               if g.created_at_api and g.created_at_api.weekday() >= 5]
    if weekend:
        sev = "high" if len(weekend) > 20 else "medium" if len(weekend) > 5 else "low"
        anomalies.append({
            "type": "WEEKEND", "severity": sev, "count": len(weekend),
            "details": f"{len(weekend)} запросов в выходные дни",
        })

    model_counts = Counter(g.model_display_name for g in generations_list if g.model_display_name)
    if model_counts:
        top3 = set(m for m, _ in model_counts.most_common(3))
        unusual = [g for g in generations_list
                   if g.model_display_name and g.model_display_name not in top3]
        if unusual:
            unusual_models = sorted(set(g.model_display_name for g in unusual))
            anomalies.append({
                "type": "UNUSUAL_MODEL", "severity": "low", "count": len(unusual),
                "details": f"Необычные модели: {', '.join(unusual_models)}",
            })

    if len(generations_list) >= 50:
        sorted_gens = sorted(
            [g for g in generations_list if g.created_at_api],
            key=lambda g: g.created_at_api,
        )
        for i in range(len(sorted_gens)):
            t0 = sorted_gens[i].created_at_api
            count = sum(
                1 for j in range(max(0, i - 1), len(sorted_gens))
                if abs((sorted_gens[j].created_at_api - t0).total_seconds()) <= 3600
            )
            if count > 50:
                anomalies.append({
                    "type": "BURST_50_PLUS", "severity": "high", "count": count,
                    "details": f"Всплеск: {count} запросов за 1 час ({t0.strftime('%d.%m %H:%M')})",
                })
                break

    return anomalies

# ─── END_BLOCK_ANOMALY


# ─── START_BLOCK_EMPLOYEE_REPORT

@employee_bp.route("/api/employee-report/list")
def api_employee_report_list():
    dbs = get_session()
    try:
        q = dbs.query(
            Generation.source_key_name,
            func.count(Generation.id).label("total_requests"),
            func.sum(Generation.cost).label("total_cost"),
            func.sum(Generation.total_tokens).label("total_tokens"),
            func.sum(Generation.cached_tokens).label("total_cached"),
            func.count(func.distinct(Generation.session_id)).label("session_count"),
        ).filter(
            Generation.source_key_name.isnot(None),
            Generation.source_key_name != "",
        )

        date_from = request.args.get("dateFrom")
        date_to = request.args.get("dateTo")
        if date_from:
            try:
                dt = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
                q = q.filter(Generation.created_at_api >= dt)
            except ValueError:
                pass
        if date_to:
            try:
                dt = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
                q = q.filter(Generation.created_at_api <= dt)
            except ValueError:
                pass

        q = q.group_by(Generation.source_key_name).order_by(desc("total_cost"))
        results = q.all()

        employees = []
        for r in results:
            employees.append({
                "name": r.source_key_name,
                "totalRequests": r.total_requests,
                "totalCost": float(r.total_cost or 0),
                "totalTokens": r.total_tokens or 0,
                "totalCached": r.total_cached or 0,
                "sessionCount": r.session_count,
            })

        return jsonify({"employees": employees, "total": len(employees)})
    except Exception as e:
        print(f"[EmployeeReport][report_list] ERROR: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        dbs.close()


@employee_bp.route("/api/employee-report")
def api_employee_report():
    employee = request.args.get("employee", "")
    date_from_str = request.args.get("dateFrom", "")
    date_to_str = request.args.get("dateTo", "")
    period = request.args.get("period", "")

    now = datetime.now(timezone.utc)
    if period == "today":
        date_from_str = now.strftime("%Y-%m-%d")
    elif period == "7d":
        date_from_str = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    elif period == "30d":
        date_from_str = (now - timedelta(days=30)).strftime("%Y-%m-%d")

    dbs = get_session()
    try:
        q = dbs.query(Generation).filter(Generation.source_key_name == employee)

        if date_from_str:
            try:
                dt = datetime.fromisoformat(date_from_str.replace("Z", "+00:00"))
                q = q.filter(Generation.created_at_api >= dt)
            except ValueError:
                pass
        if date_to_str:
            try:
                dt = datetime.fromisoformat(date_to_str.replace("Z", "+00:00"))
                q = q.filter(Generation.created_at_api <= dt)
            except ValueError:
                pass

        generations = q.order_by(desc(Generation.created_at_api)).all()

        total_cost = sum(g.cost or 0 for g in generations)
        total_tokens = sum(g.total_tokens or 0 for g in generations)
        total_cached = sum(g.cached_tokens or 0 for g in generations)
        total_completion = sum(g.completion_tokens or 0 for g in generations)

        sessions_map = {}
        models_counter = Counter()
        for g in generations:
            sid = g.session_id or ""
            if sid not in sessions_map:
                sessions_map[sid] = {
                    "sessionId": sid,
                    "sourceKey": g.source_key_name,
                    "firstAt": None, "lastAt": None,
                    "totalCount": 0, "totalCost": 0,
                    "models": set(),
                }
            s = sessions_map[sid]
            s["totalCount"] += 1
            s["totalCost"] += g.cost or 0
            if g.created_at_api:
                if not s["firstAt"] or g.created_at_api < s["firstAt"]:
                    s["firstAt"] = g.created_at_api
                if not s["lastAt"] or g.created_at_api > s["lastAt"]:
                    s["lastAt"] = g.created_at_api
            if g.model_display_name:
                s["models"].add(g.model_display_name)
                models_counter[g.model_display_name] += 1

        sessions = []
        for sid, s in sessions_map.items():
            s["models"] = sorted(s["models"])
            s["firstAt"] = s["firstAt"].isoformat() if s["firstAt"] else None
            s["lastAt"] = s["lastAt"].isoformat() if s["lastAt"] else None
            s["totalCost"] = float(s["totalCost"])
            if sid:
                cached_summary = summary_get_or_none(sid)
                if cached_summary:
                    s["_summary"] = cached_summary.to_dict()
            sessions.append(s)
        sessions.sort(key=lambda x: x["lastAt"] or "", reverse=True)

        anomalies = detect_anomalies(generations)

        heatmap = [[0] * 24 for _ in range(7)]
        for g in generations:
            if g.created_at_api:
                heatmap[g.created_at_api.weekday()][g.created_at_api.hour] += 1

        models = [{"name": m, "count": c} for m, c in models_counter.most_common(10)]

        print(f"[EmployeeReport][report] returned {len(generations)} gens, {len(sessions)} sessions, {len(anomalies)} anomalies for {employee}")
        return jsonify({
            "employee": employee,
            "totals": {
                "cost": float(total_cost),
                "tokens": total_tokens,
                "cached": total_cached,
                "completion": total_completion,
                "requests": len(generations),
                "sessions": len([s for s in sessions if s["sessionId"]]),
            },
            "sessions": sessions,
            "anomalies": anomalies,
            "heatmap": heatmap,
            "models": models,
        })
    except Exception as e:
        print(f"[EmployeeReport][report] ERROR: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        dbs.close()

# ─── END_BLOCK_EMPLOYEE_REPORT
