"""
M-RAG-CONTEXT-BUILDER: Mode-specific context building for RAG.
Converts retrieved sources into formatted context blocks for LLM prompt.
Strategy pattern: different formatting per QueryMode.
"""
from typing import List, Any
from rag.retriever import QueryMode
from db import GenerationSummary
from sqlalchemy.orm import Session


# START_MODULE_CONTRACT
# PURPOSE: Build mode-specific context blocks from retrieved sources for RAG chat prompt
# SCOPE: Context formatting (SEARCH, DOSSIER, EMPLOYEE_LIST, GLOBAL_AGG modes)
# INPUTS: mode: QueryMode, sources: List[Any], query: str
# OUTPUTS: formatted context block: str
# DEPENDS: rag.retriever, db.py (GenerationSummary, Session)
# LINKS: M-RAG-RETRIEVER, M-RAG-CHAT
# END_MODULE_CONTRACT


# START_BLOCK_CONTEXT_BUILDER_CLASS
class ContextBuilder:
    """Build mode-specific context for LLM prompt."""
    
    def __init__(self, mode: QueryMode, db_session: Session = None):
        """
        Initialize builder for specific mode.
        
        Args:
            mode: QueryMode enum value
            db_session: SQLAlchemy session for DB queries (optional, for EMPLOYEE_LIST/GLOBAL_AGG)
        """
        self.mode = mode
        self.db = db_session
    
    def build(self, query: str, sources: List[Any]) -> str:
        """
        Build formatted context block.
        
        Returns string suitable for inclusion in LLM system prompt.
        """
        if self.mode == QueryMode.SEARCH:
            return self._build_search_context(sources)
        elif self.mode == QueryMode.DOSSIER:
            return self._build_dossier_context(sources)
        elif self.mode == QueryMode.EMPLOYEE_LIST:
            return self._build_employee_list_context()
        elif self.mode == QueryMode.GLOBAL_AGG:
            return self._build_global_agg_context()
        else:
            return ""
    
    def _build_search_context(self, sources: List[Any]) -> str:
        """Format semantic search results (top 10 with snippets)."""
        if not sources:
            return "Нет источников для данного запроса."
        
        # START_BLOCK_SEARCH_FORMAT
        context = "**Источники (семантический поиск):**\n\n"
        for i, src in enumerate(sources[:10], 1):
            # Each source should have: generation_id, topic, snippet, score
            snippet = getattr(src, "snippet", "")[:200]  # Cap at 200 chars
            topic = getattr(src, "topic", "N/A")
            score = getattr(src, "score", 0.0)
            context += f"{i}. {topic} (релевантность: {score:.2f})\n   {snippet}\n\n"
        return context
        # END_BLOCK_SEARCH_FORMAT
    
    def _build_dossier_context(self, sources: List[Any]) -> str:
        """Format employee dossier (all sources for one employee)."""
        if not sources:
            return "Информация о сотруднике не найдена."
        
        # START_BLOCK_DOSSIER_FORMAT
        # Group sources by employee (if available)
        context = "**Профиль сотрудника:**\n\n"
        context += f"Всего записей: {len(sources)}\n"
        
        # Sample stats from first source
        if sources:
            src = sources[0]
            employee = getattr(src, "employee_name", "Unknown")
            total_cost = sum(float(getattr(s, "cost", 0)) for s in sources)
            context += f"\nСотрудник: {employee}\n"
            context += f"Общие затраты: ${total_cost:.2f}\n"
        
        context += "\nПоследние записи:\n"
        for src in sources[:5]:
            snippet = getattr(src, "snippet", "")[:300]
            created = getattr(src, "created_at", "N/A")
            context += f"- {created}: {snippet}\n"
        
        return context
        # END_BLOCK_DOSSIER_FORMAT
    
    def _build_employee_list_context(self) -> str:
        """Format employee list with is_work stats."""
        if not self.db:
            return "Список сотрудников недоступен (нет сессии БД)."
        
        # START_BLOCK_EMPLOYEE_LIST_FORMAT
        try:
            # Query employee list with aggregated stats
            from sqlalchemy import func, text
            
            query = text("""
                SELECT DISTINCT key_name, COUNT(*) as count, 
                       AVG(CASE WHEN is_work THEN 1 ELSE 0 END) as work_ratio
                FROM generation_summaries
                GROUP BY key_name
                ORDER BY count DESC
            """)
            
            results = self.db.execute(query).fetchall()
            context = "**Список сотрудников команды:**\n\n"
            for row in results:
                name, count, ratio = row
                context += f"- {name}: {count} запросов ({ratio*100:.0f}% рабочих)\n"
            
            return context
        except Exception as e:
            return f"Ошибка при загрузке списка: {e}"
        # END_BLOCK_EMPLOYEE_LIST_FORMAT
    
    def _build_global_agg_context(self) -> str:
        """Format aggregate metrics table."""
        if not self.db:
            return "Агрегированная статистика недоступна (нет сессии БД)."
        
        # START_BLOCK_GLOBAL_AGG_FORMAT
        try:
            from sqlalchemy import func, text
            
            query = text("""
                SELECT 
                    COUNT(*) as total_gens,
                    COUNT(DISTINCT key_name) as unique_employees,
                    AVG(cost) as avg_cost,
                    SUM(cost) as total_cost,
                    AVG(tokens) as avg_tokens
                FROM generation_summaries
            """)
            
            row = self.db.execute(query).fetchone()
            if not row:
                return "Нет данных для статистики."
            
            total_gens, emp_count, avg_cost, total_cost, avg_tokens = row
            
            context = "**Глобальная статистика:**\n\n"
            context += f"- Всего генераций: {total_gens}\n"
            context += f"- Уникальных сотрудников: {emp_count}\n"
            context += f"- Средние затраты на запрос: ${avg_cost:.2f}\n"
            context += f"- Всего затрат: ${total_cost:.2f}\n"
            context += f"- Средние токены: {avg_tokens:.0f}\n"
            
            return context
        except Exception as e:
            return f"Ошибка при расчете статистики: {e}"
        # END_BLOCK_GLOBAL_AGG_FORMAT
# END_BLOCK_CONTEXT_BUILDER_CLASS
