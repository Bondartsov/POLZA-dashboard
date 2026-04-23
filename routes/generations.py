from flask import Blueprint, jsonify, request
from datetime import datetime
from sqlalchemy import desc, asc

from config import get_session, Generation, _provider_state

gens_bp = Blueprint('generations', __name__)


def _apply_filters(query, args):
    date_from = args.get("dateFrom")
    date_to = args.get("dateTo")
    req_type = args.get("requestType")
    status = args.get("status")
    key_name = args.get("keyName")
    search = args.get("search", "").lower()

    if date_from:
        try:
            dt = datetime.fromisoformat(date_from.replace("Z", "+00:00"))
            query = query.filter(Generation.created_at_api >= dt)
        except ValueError:
            pass
    if date_to:
        try:
            dt = datetime.fromisoformat(date_to.replace("Z", "+00:00"))
            query = query.filter(Generation.created_at_api <= dt)
        except ValueError:
            pass
    if req_type:
        query = query.filter(Generation.request_type == req_type)
    if status:
        query = query.filter(Generation.status == status)
    if key_name:
        query = query.filter(
            (Generation.api_key_name == key_name) |
            (Generation.source_key_name == key_name)
        )
    if search:
        query = query.filter(
            (Generation.model_display_name.ilike(f"%{search}%")) |
            (Generation.api_key_name.ilike(f"%{search}%")) |
            (Generation.id.ilike(f"%{search}%"))
        )
    return query


@gens_bp.route("/api/db/all")
def api_db_all():
    session = get_session()
    try:
        q = _apply_filters(session.query(Generation), request.args)
        q = q.order_by(desc(Generation.created_at_api))
        items = [g.to_dict() for g in q.all()]
        return jsonify({
            "items": items,
            "meta": {"total": len(items), "totalPages": 1, "page": 1, "limit": len(items)}
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()


@gens_bp.route("/api/db/generations")
def api_db_generations():
    session = get_session()
    try:
        q = _apply_filters(session.query(Generation), request.args)
        total = q.count()

        sort_by = request.args.get("sortBy", "createdAt")
        sort_order = request.args.get("sortOrder", "desc")
        sort_col = {
            "createdAt": Generation.created_at_api,
            "cost": Generation.cost,
            "clientCost": Generation.client_cost,
        }.get(sort_by, Generation.created_at_api)

        q = q.order_by(asc(sort_col) if sort_order == "asc" else desc(sort_col))

        page = int(request.args.get("page", 1))
        limit = int(request.args.get("limit", 50))
        q = q.offset((page - 1) * limit).limit(limit)

        items = [g.to_dict() for g in q.all()]
        total_pages = max(1, (total + limit - 1) // limit)

        return jsonify({
            "items": items,
            "meta": {"total": total, "totalPages": total_pages, "page": page, "limit": limit}
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        session.close()
