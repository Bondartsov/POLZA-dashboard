# FILE: rag/__init__.py
# VERSION: 1.1.0
# START_MODULE_CONTRACT
#   PURPOSE: Re-exports for RAG package
#   SCOPE: Public API of rag package (search, chat, retrieval, context, guardrails)
#   DEPENDS: rag.search, rag.chat, rag.retriever, rag.context_builder, rag.guardrails
#   LINKS: M-RAG-SEARCH, M-RAG-CHAT, M-RAG-RETRIEVER, M-RAG-CONTEXT-BUILDER, M-RAG-GUARDRAILS
# END_MODULE_CONTRACT

# Core RAG modules
from rag.search import _rag_search, _detect_employee_filter, _get_employee_names
from rag.prompts import RAG_SYSTEM_PROMPT, _build_context_block
from rag.chat import _chat_new_session, _chat_add_message, _chat_get_session

# Phase-7 Wave-2 modules
from rag.retriever import QueryMode, QueryClassifier, retrieve
from rag.context_builder import ContextBuilder
from rag.guardrails import _escape_xml, _detect_injection_attempt, _redact_pii, validate_and_sanitize
