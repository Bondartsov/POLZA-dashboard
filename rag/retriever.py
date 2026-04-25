"""
M-RAG-RETRIEVER: Query classification and retrieval mode detection.
Classifies user queries into 4 modes: SEARCH, DOSSIER, EMPLOYEE_LIST, GLOBAL_AGG.
Enables mode-specific context building and answer generation.
"""
from enum import Enum
from typing import List, Tuple, Any
from rag.search import _detect_employee_filter, _rag_search
from config import RAG_MAX_SOURCES


# START_MODULE_CONTRACT
# PURPOSE: Classify RAG queries into one of 4 retrieval modes for targeted context building
# SCOPE: Query classification (keywords + heuristics), mode-specific retrieval
# INPUTS: user_query: str
# OUTPUTS: QueryMode enum + retrieved sources list
# DEPENDS: rag.search, config
# LINKS: M-RAG-CHAT, M-RAG-CONTEXT-BUILDER
# END_MODULE_CONTRACT


class QueryMode(Enum):
    """4 query modes for RAG pipeline."""
    SEARCH = "search"  # General semantic search + employee filter
    DOSSIER = "dossier"  # Deep dive into single employee
    EMPLOYEE_LIST = "employee_list"  # List with stats
    GLOBAL_AGG = "global_agg"  # Aggregate metrics


# START_BLOCK_QUERY_CLASSIFIER
import re

# WAVE-2 FIX: Cache keywords and compile regex patterns at module level (avoid recreating on each classification)
# Also enables future scoring-based classification and detection of keyword stuffing attempts

_CLASSIFIER_KEYWORDS = {
    QueryMode.DOSSIER: [
        "расскажи о сотруднике", "профиль", "дневник", "история",
        "что делает", "чем занимался", "личные данные", "информация о",
        "who is", "tell me about", "profile", "history", "log", "досье",
    ],
    QueryMode.EMPLOYEE_LIST: [
        "список сотрудников", "сотрудники", "team", "team list", "employees",
        "кто в команде", "список команды", "все люди",
    ],
    QueryMode.GLOBAL_AGG: [
        "всего затрат", "итого", "средние затраты", "total cost", "average",
        "статистика команды", "сводка", "overview", "summary",
        "бюджет", "budget", "cost breakdown",
    ],
}

# Precompile regex patterns for word-boundary matching (prevents keyword stuffing)
_CLASSIFIER_PATTERNS = {}
for mode, keywords in _CLASSIFIER_KEYWORDS.items():
    # Word-boundary regex: \b keyword \b (case-insensitive)
    _CLASSIFIER_PATTERNS[mode] = [
        re.compile(r'\b' + re.escape(kw) + r'\b', re.IGNORECASE)
        for kw in keywords
    ]

# Priority order for classification (DOSSIER > EMPLOYEE_LIST > GLOBAL_AGG > SEARCH)
_MODE_PRIORITY = [QueryMode.DOSSIER, QueryMode.EMPLOYEE_LIST, QueryMode.GLOBAL_AGG]


class QueryClassifier:
    """Classify queries into retrieval modes.
    
    WAVE-2: Optimized with cached keywords and precompiled patterns.
    Detects exact word matches (word boundaries) to prevent simple keyword stuffing.
    """
    
    def classify(self, query: str) -> QueryMode:
        """Classify query into one of 4 modes with word-boundary matching.
        
        Algorithm:
        1. Check each mode in priority order (DOSSIER → EMPLOYEE_LIST → GLOBAL_AGG)
        2. For each mode, count matching keywords (word boundaries)
        3. Return first mode with matches, or SEARCH if none
        
        Word-boundary matching prevents keyword stuffing like "dossier employee_list_global"
        (would match "list" in "employee_list" substring, but not with boundaries).
        """
        if not query:
            return QueryMode.SEARCH
        
        # Check each mode in priority order
        for mode in _MODE_PRIORITY:
            patterns = _CLASSIFIER_PATTERNS[mode]
            # Check if ANY pattern matches (word boundary)
            for pattern in patterns:
                if pattern.search(query):
                    return mode
        
        return QueryMode.SEARCH
# END_BLOCK_QUERY_CLASSIFIER


# START_BLOCK_RETRIEVE_DISPATCHER
_classifier = QueryClassifier()  # Singleton


def retrieve(query: str) -> Tuple[QueryMode, List[Any]]:
    """Classify query and retrieve relevant sources.
    
    Returns (mode, sources) where sources is mode-specific:
    - SEARCH/DOSSIER: semantic search results from Qdrant
    - EMPLOYEE_LIST: employee list from database
    - GLOBAL_AGG: aggregate metrics (TBD by context builder)
    """
    mode = _classifier.classify(query)
    
    # Common: check if employee filter needed
    employee_names = _detect_employee_filter(query)
    
    if mode == QueryMode.SEARCH or mode == QueryMode.DOSSIER:
        # Use main RAG search function (handles all modes internally)
        search_result = _rag_search(query)
        sources = search_result.get("sources", [])
    elif mode == QueryMode.EMPLOYEE_LIST:
        # Employee list retrieval (handled by context builder)
        sources = []
    elif mode == QueryMode.GLOBAL_AGG:
        # Aggregate metrics (handled by context builder)
        sources = []
    else:
        sources = []
    
    return mode, sources
# END_BLOCK_RETRIEVE_DISPATCHER
