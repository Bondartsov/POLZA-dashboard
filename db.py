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
import os
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, Float, Boolean, DateTime, JSON, Index
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
    synced_at = Column(DateTime(timezone=True), default=datetime.utcnow)

    __table_args__ = (
        Index("idx_gen_created_at", "created_at_api"),
        Index("idx_gen_source_key", "source_key_name"),
        Index("idx_gen_model", "model_display_name"),
        Index("idx_gen_status", "status"),
        Index("idx_gen_request_type", "request_type"),
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
        }


# ─── Helpers ─────────────────────────────────────────────────────────────────────

def init_db():
    """Create tables if not exist."""
    Base.metadata.create_all(engine)


def get_session():
    return SessionLocal()
