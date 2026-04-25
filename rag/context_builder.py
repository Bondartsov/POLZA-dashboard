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
            # WAVE-2 FIX: Use parameterized queries to prevent SQL injection
            from sqlalchemy import func, select
            from db import GenerationSummary
            
            # ORM-based query (parameterized, safe)
            stmt = select(
                GenerationSummary.key_name,
                func.count(GenerationSummary.id).label("count"),
                func.avg(func.cast(GenerationSummary.is_work, int)).label("work_ratio")
            ).group_by(GenerationSummary.key_name).order_by(func.count(GenerationSummary.id).desc())
            
            results = self.db.execute(stmt).fetchall()
            context = "**Список сотрудников команды:**\n\n"
            for row in results:
                name, count, ratio = row
                ratio_val = (ratio or 0) * 100
                context += f"- {name}: {count} запросов ({ratio_val:.0f}% рабочих)\n"
            
            return context
        except (ValueError, TypeError, AttributeError) as e:
            import logging
            logging.error("Context building failed for EMPLOYEE_LIST mode", exc_info=True)
            return "Ошибка при загрузке списка сотрудников (некорректные данные)"
        except Exception as e:
            import logging
            logging.critical("Unexpected error in EMPLOYEE_LIST context", exc_info=True)
            return "Системная ошибка при загрузке списка"
        # END_BLOCK_EMPLOYEE_LIST_FORMAT
    
    def _build_global_agg_context(self) -> str:
        """Format aggregate metrics table."""
        if not self.db:
            return "Агрегированная статистика недоступна (нет сессии БД)."
        
        # START_BLOCK_GLOBAL_AGG_FORMAT
        try:
            # WAVE-2 FIX: Use parameterized ORM queries (safe from SQL injection)
            from sqlalchemy import func, select
            from db import GenerationSummary
            
            # ORM-based aggregation (parameterized, safe)
            total_gens = self.db.query(func.count(GenerationSummary.id)).scalar() or 0
            emp_count = self.db.query(func.count(func.distinct(GenerationSummary.key_name))).scalar() or 0
            avg_cost = self.db.query(func.avg(GenerationSummary.cost)).scalar() or 0.0
            total_cost = self.db.query(func.sum(GenerationSummary.cost)).scalar() or 0.0
            avg_tokens = self.db.query(func.avg(GenerationSummary.tokens)).scalar() or 0.0
            
            if not total_gens:
                return "Нет данных для статистики."
            
            context = "**Глобальная статистика:**\n\n"
            context += f"- Всего генераций: {total_gens}\n"
            context += f"- Уникальных сотрудников: {emp_count}\n"
            context += f"- Средние затраты на запрос: ${float(avg_cost):.2f}\n"
            context += f"- Всего затрат: ${float(total_cost):.2f}\n"
            context += f"- Средние токены: {float(avg_tokens):.0f}\n"
            
            return context
        except (ValueError, TypeError, AttributeError) as e:
            import logging
            logging.error("Context building failed for GLOBAL_AGG mode", exc_info=True)
            return "Ошибка при расчете статистики (некорректные данные)"
        except Exception as e:
            import logging
            logging.critical("Unexpected error in GLOBAL_AGG context", exc_info=True)
            return "Системная ошибка при расчете статистики"
        # END_BLOCK_GLOBAL_AGG_FORMAT
# END_BLOCK_CONTEXT_BUILDER_CLASS
