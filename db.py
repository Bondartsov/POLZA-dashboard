#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Polza.AI Dashboard — Database models and connection.
START_MODULE_CONTRACT
  PURPOSE: SQLAlchemy ORM models for PostgreSQL caching layer
  SCOPE: ApiKey + Generation models, session management, init
  DEPENDS: SQLAlchemy, psycopg2-binary
  LINKS: M-DB
END_MODULE_CONTRACT
"""
import json
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, DateTime, JSON, Index, Text
from sqlalchemy.orm import declarative_base, sessionmaker

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://dbadmin:bond@localhost:5432/polza_dashboard"
)

engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()


# ─── Models ──────────────────────────────────────────────────────────────────────

class ApiKey(Base):
    """Registered API key with sync metadata."""
    __tablename__ = "api_keys"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), nullable=False)
    token = Column(String(500), nullable=False, unique=True)
    key_suffix = Column(String(10), nullable=False)
    is_primary = Column(Boolean, default=False)
    last_sync_at = Column(DateTime(timezone=True), nullable=True)
    total_synced = Column(Integer, default=0)
    last_error = Column(String(500), nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    def to_dict(self):
        return {
            "name": self.name,
            "keySuffix": self.key_suffix,
            "isPrimary": self.is_primary,
            "lastSyncAt": self.last_sync_at.isoformat() if self.last_sync_at else None,
            "totalSynced": self.total_synced,
            "lastError": self.last_error,
        }


class Generation(Base):
    """Cached generation record from Polza.AI API."""
    __tablename__ = "generations"

    id = Column(String(100), primary_key=True)
    model = Column(String(255))
    model_display_name = Column(String(255))
    request_type = Column(String(50))
    status = Column(String(50))
    cost = Column(Float)
    client_cost = Column(Float)
    prompt_tokens = Column(Integer, default=0)
    completion_tokens = Column(Integer, default=0)
    total_tokens = Column(Integer, default=0)
    cached_tokens = Column(Integer, default=0)
    reasoning_tokens = Column(Integer, default=0)
    audio_tokens = Column(Integer, default=0)
    video_tokens = Column(Integer, default=0)
    generation_time_ms = Column(Integer)
    latency_ms = Column(Integer)
    created_at_api = Column(DateTime(timezone=True))
    completed_at = Column(DateTime(timezone=True))
    api_key_name = Column(String(255))
    api_key_short = Column(String(50))
    api_key_id = Column(String(100))
    finish_reason = Column(String(100))
    response_mode = Column(String(50))
    has_log = Column(Boolean, default=False)
    final_endpoint_slug = Column(String(100))
    api_type = Column(String(50))
    provider = Column(String(100))
    source_key_name = Column(String(255))
    usage_data = Column(JSON)
    session_id = Column(String(100), nullable=True)
    device_id = Column(String(128), nullable=True)
    synced_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("idx_gen_created_at", "created_at_api"),
        Index("idx_gen_source_key", "source_key_name"),
        Index("idx_gen_model", "model_display_name"),
        Index("idx_gen_status", "status"),
        Index("idx_gen_request_type", "request_type"),
        Index("idx_gen_session_id", "session_id"),
    )

    def to_dict(self):
        """Serialize to same format as Polza.AI API response."""
        return {
            "id": self.id,
            "model": self.model,
            "modelDisplayName": self.model_display_name,
            "requestType": self.request_type,
            "status": self.status,
            "cost": self.cost,
            "clientCost": self.client_cost,
            "usage": self.usage_data or {},
            "generationTimeMs": self.generation_time_ms,
            "latencyMs": self.latency_ms,
            "createdAt": self.created_at_api.isoformat() if self.created_at_api else None,
            "completedAt": self.completed_at.isoformat() if self.completed_at else None,
            "apiKeyName": self.api_key_name,
            "apiKeyShort": self.api_key_short,
            "apiKeyId": self.api_key_id,
            "finishReason": self.finish_reason,
            "responseMode": self.response_mode,
            "hasLog": self.has_log,
            "finalEndpointSlug": self.final_endpoint_slug,
            "apiType": self.api_type,
            "provider": self.provider,
            "_sourceKey": self.source_key_name,
            "_sessionId": self.session_id,
            "_deviceId": self.device_id,
        }


# ─── START_BLOCK_SESSION_SUMMARY
# M-SUMMARY-STORE: LLM-generated session summaries cache

class SessionSummary(Base):
    """LLM-generated summary for a chat session."""
    __tablename__ = "session_summaries"

    session_id = Column(String(100), primary_key=True)
    source_key = Column(String(255))
    summary = Column(Text)
    topic = Column(String(255))
    is_work = Column(Boolean)
    project_guess = Column(String(255))
    risk_flags = Column(Text)  # JSON array stored as text
    prompt_hashes = Column(Text)  # JSON array stored as text
    llm_cost = Column(Float, default=0.0)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "sessionId": self.session_id,
            "sourceKey": self.source_key,
            "summary": self.summary,
            "topic": self.topic,
            "isWork": self.is_work,
            "projectGuess": self.project_guess,
            "riskFlags": json.loads(self.risk_flags) if self.risk_flags else [],
            "promptHashes": json.loads(self.prompt_hashes) if self.prompt_hashes else [],
            "llmCost": self.llm_cost,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
        }

# ─── END_BLOCK_SESSION_SUMMARY


# ─── START_BLOCK_GEN_SUMMARY
# M-GEN-SUMMARY-STORE: LLM-generated per-generation summaries cache

class GenerationSummary(Base):
    """LLM-generated summary for a single generation (persistent cache)."""
    __tablename__ = "generation_summaries"

    generation_id = Column(String(100), primary_key=True)
    summary = Column(Text)
    topic = Column(String(255))
    is_work = Column(Boolean)
    project_guess = Column(String(500))
    risk_flags = Column(Text)  # JSON array stored as text
    llm_model = Column(String(255))
    llm_cost = Column(Float, default=0.0)
    cache_creation_tokens = Column(Integer, default=0)
    cache_read_tokens = Column(Integer, default=0)
    input_tokens = Column(Integer, default=0)
    output_tokens = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at = Column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("idx_gensum_created", "created_at"),
    )

    def to_dict(self):
        return {
            "generationId": self.generation_id,
            "summary": self.summary,
            "topic": self.topic,
            "isWork": self.is_work,
            "projectGuess": self.project_guess,
            "riskFlags": json.loads(self.risk_flags) if self.risk_flags else [],
            "llmModel": self.llm_model,
            "llmCost": self.llm_cost,
            "cacheCreationTokens": self.cache_creation_tokens or 0,
            "cacheReadTokens": self.cache_read_tokens or 0,
            "inputTokens": self.input_tokens or 0,
            "outputTokens": self.output_tokens or 0,
            "createdAt": self.created_at.isoformat() if self.created_at else None,
            "updatedAt": self.updated_at.isoformat() if self.updated_at else None,
            "cached": True,
        }

# ─── END_BLOCK_GEN_SUMMARY


# ─── Helpers ─────────────────────────────────────────────────────────────────────

def init_db():
    """Create tables if not exist."""
    Base.metadata.create_all(engine)


def get_session():
    return SessionLocal()


# ─── START_BLOCK_SUMMARY_CRUD
# M-SUMMARY-STORE: CRUD operations

def summary_get_or_none(session_id: str):
    """Get summary by session_id, return None if not cached."""
    s = get_session()
    try:
        return s.query(SessionSummary).filter(
            SessionSummary.session_id == session_id
        ).first()
    finally:
        s.close()


def summary_upsert(session_id, source_key, summary, topic, is_work,
                   project_guess=None, risk_flags=None,
                   prompt_hashes=None, llm_cost=0.0):
    """INSERT or UPDATE summary for a session_id."""
    s = get_session()
    try:
        existing = s.query(SessionSummary).filter(
            SessionSummary.session_id == session_id
        ).first()
        if existing:
            existing.source_key = source_key
            existing.summary = summary
            existing.topic = topic
            existing.is_work = is_work
            existing.project_guess = project_guess
            existing.risk_flags = json.dumps(risk_flags or [], ensure_ascii=False)
            existing.prompt_hashes = json.dumps(prompt_hashes or [], ensure_ascii=False)
            existing.llm_cost = llm_cost
            existing.updated_at = datetime.utcnow()
        else:
            row = SessionSummary(
                session_id=session_id,
                source_key=source_key,
                summary=summary,
                topic=topic,
                is_work=is_work,
                project_guess=project_guess,
                risk_flags=json.dumps(risk_flags or [], ensure_ascii=False),
                prompt_hashes=json.dumps(prompt_hashes or [], ensure_ascii=False),
                llm_cost=llm_cost,
            )
            s.add(row)
        s.commit()
        print(f"[SummaryStore][upsert] cached session_id={session_id[:16]}")
    except Exception as e:
        s.rollback()
        print(f"[SummaryStore][upsert] ERROR: {e}")
        raise
    finally:
        s.close()


def summary_list_by_key(source_key: str, date_from=None, date_to=None):
    """List summaries for an employee within date range."""
    s = get_session()
    try:
        q = s.query(SessionSummary).filter(SessionSummary.source_key == source_key)
        if date_from:
            q = q.filter(SessionSummary.created_at >= date_from)
        if date_to:
            q = q.filter(SessionSummary.created_at <= date_to)
        return q.all()
    finally:
        s.close()

# ─── END_BLOCK_SUMMARY_CRUD


# ─── START_BLOCK_GEN_SUMMARY_CRUD
# M-GEN-SUMMARY-STORE: per-generation cache CRUD

def gen_summary_get_or_none(generation_id: str):
    """Fetch cached summary for a generation, or None."""
    s = get_session()
    try:
        return s.query(GenerationSummary).filter(
            GenerationSummary.generation_id == generation_id
        ).first()
    finally:
        s.close()


def gen_summary_get_many(generation_ids):
    """Fetch multiple cached summaries by a list of generation IDs. Returns dict {id: dict}."""
    if not generation_ids:
        return {}
    s = get_session()
    try:
        rows = s.query(GenerationSummary).filter(
            GenerationSummary.generation_id.in_(list(generation_ids))
        ).all()
        return {row.generation_id: row.to_dict() for row in rows}
    finally:
        s.close()


def gen_summary_upsert(generation_id, summary, topic, is_work,
                       project_guess=None, risk_flags=None,
                       llm_model=None, llm_cost=0.0,
                       cache_creation_tokens=0, cache_read_tokens=0,
                       input_tokens=0, output_tokens=0):
    """INSERT or UPDATE summary for a generation_id."""
    s = get_session()
    try:
        existing = s.query(GenerationSummary).filter(
            GenerationSummary.generation_id == generation_id
        ).first()
        if existing:
            existing.summary = summary
            existing.topic = topic
            existing.is_work = is_work
            existing.project_guess = project_guess
            existing.risk_flags = json.dumps(risk_flags or [], ensure_ascii=False)
            existing.llm_model = llm_model
            existing.llm_cost = llm_cost
            existing.cache_creation_tokens = cache_creation_tokens
            existing.cache_read_tokens = cache_read_tokens
            existing.input_tokens = input_tokens
            existing.output_tokens = output_tokens
            existing.updated_at = datetime.utcnow()
        else:
            row = GenerationSummary(
                generation_id=generation_id,
                summary=summary,
                topic=topic,
                is_work=is_work,
                project_guess=project_guess,
                risk_flags=json.dumps(risk_flags or [], ensure_ascii=False),
                llm_model=llm_model,
                llm_cost=llm_cost,
                cache_creation_tokens=cache_creation_tokens,
                cache_read_tokens=cache_read_tokens,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
            s.add(row)
        s.commit()
        print(f"[GenSummaryStore][upsert] cached generation_id={generation_id[:16]}")
    except Exception as e:
        s.rollback()
        print(f"[GenSummaryStore][upsert] ERROR: {e}")
        raise
    finally:
        s.close()


def gen_summary_delete(generation_id: str):
    """Remove cached summary — triggers regeneration on next request."""
    s = get_session()
    try:
        s.query(GenerationSummary).filter(
            GenerationSummary.generation_id == generation_id
        ).delete()
        s.commit()
    finally:
        s.close()

# ─── END_BLOCK_GEN_SUMMARY_CRUD


# ─── START_BLOCK_ANALYSIS_STATE
# Persistent state for analyze-all background job (survives restarts)

class AnalysisState(Base):
    """Singleton row: tracks analyze-all progress. Survives server restart."""
    __tablename__ = "analysis_state"

    id = Column(Integer, primary_key=True, default=1)  # always id=1
    status = Column(String(20), default="idle")  # idle | running | paused | completed | error
    total = Column(Integer, default=0)
    done = Column(Integer, default=0)
    skipped = Column(Integer, default=0)
    errors = Column(Integer, default=0)
    started_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), nullable=True)
    completed_at = Column(DateTime(timezone=True), nullable=True)

def get_analysis_state():
    """Get or create the singleton analysis state row."""
    s = get_session()
    try:
        row = s.query(AnalysisState).filter(AnalysisState.id == 1).first()
        if not row:
            row = AnalysisState(id=1, status="idle")
            s.add(row)
            s.commit()
        return row
    finally:
        s.close()

def update_analysis_state(**kwargs):
    """Update analysis state fields atomically."""
    s = get_session()
    try:
        row = s.query(AnalysisState).filter(AnalysisState.id == 1).first()
        if not row:
            row = AnalysisState(id=1)
            s.add(row)
        for k, v in kwargs.items():
            setattr(row, k, v)
        row.updated_at = datetime.utcnow()
        s.commit()
        return row
    except Exception as e:
        s.rollback()
        print(f"[AnalysisState] update error: {e}")
        raise
    finally:
        s.close()

def get_analysis_counts():
    """Return {total, analyzed, remaining} counts from DB."""
    s = get_session()
    try:
        total = s.query(Generation).count()
        analyzed = s.query(GenerationSummary).count()
        return {"total": total, "analyzed": analyzed, "remaining": max(0, total - analyzed)}
    finally:
        s.close()

# ─── END_BLOCK_ANALYSIS_STATE
