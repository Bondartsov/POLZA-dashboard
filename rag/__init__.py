# FILE: rag/__init__.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
#   PURPOSE: Re-exports for RAG package
#   SCOPE: Public API of rag package
#   DEPENDS: rag.search, rag.chat
#   LINKS: M-RAG-SEARCH, M-RAG-CHAT
# END_MODULE_CONTRACT

from rag.search import _rag_search, _detect_employee_filter, _get_employee_names
from rag.prompts import RAG_SYSTEM_PROMPT, _build_context_block
