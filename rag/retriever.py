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
def _get_dossier_keywords() -> List[str]:
    """Keywords that trigger DOSSIER mode."""
    return [
        "расскажи о сотруднике", "профиль", "дневник", "история",
        "что делает", "чем занимался", "личные данные", "информация о",
        "who is", "tell me about", "profile", "history", "log",
    ]


def _get_employee_list_keywords() -> List[str]:
    """Keywords that trigger EMPLOYEE_LIST mode."""
    return [
        "список сотрудников", "сотрудники", "team", "team list", "employees",
        "кто в команде", "список команды", "все люди",
    ]


def _get_global_agg_keywords() -> List[str]:
    """Keywords that trigger GLOBAL_AGG mode."""
    return [
        "всего затрат", "итого", "средние затраты", "total cost", "average",
        "статистика команды", "сводка", "overview", "summary",
        "бюджет", "budget", "cost breakdown",
    ]


class QueryClassifier:
    """Classify queries into retrieval modes."""
    
    def __init__(self):
        self.dossier_kw = _get_dossier_keywords()
        self.employee_list_kw = _get_employee_list_keywords()
        self.global_agg_kw = _get_global_agg_keywords()
    
    def classify(self, query: str) -> QueryMode:
        """Classify query into one of 4 modes.
        
        Rules:
        1. Check for DOSSIER keywords (highest priority)
        2. Check for EMPLOYEE_LIST keywords
        3. Check for GLOBAL_AGG keywords
        4. Default to SEARCH
        """
        query_lower = query.lower()
        
        # Priority: specific modes first, fallback to SEARCH
        for kw in self.dossier_kw:
            if kw in query_lower:
                return QueryMode.DOSSIER
        
        for kw in self.employee_list_kw:
            if kw in query_lower:
                return QueryMode.EMPLOYEE_LIST
        
        for kw in self.global_agg_kw:
            if kw in query_lower:
                return QueryMode.GLOBAL_AGG
        
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
