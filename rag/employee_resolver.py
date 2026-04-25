# FILE: rag/employee_resolver.py
# VERSION: 1.0.0
# START_MODULE_CONTRACT
#   PURPOSE: Robust employee name resolution with fuzzy matching (fixes Issue #1, #9)
#   SCOPE: Exact match, fuzzy match, partial match, confidence scoring
#   DEPENDS: M-CONFIG, M-DB
#   LINKS: M-RAG-SEARCH (replaces _detect_employee_filter)
# END_MODULE_CONTRACT

import difflib
from typing import List, Optional, Tuple
from dataclasses import dataclass


@dataclass
class EmployeeMatch:
    """Result of employee name resolution attempt."""
    name: str
    confidence: float  # 0.0 - 1.0
    method: str  # 'exact', 'fuzzy', 'partial'


class EmployeeResolver:
    """Resolve employee names from queries with fuzzy matching.
    
    Fixes Issue #1 (substring matching collisions) and Issue #9 (multiple names).
    Uses difflib.SequenceMatcher for fuzzy matching (no external deps).
    """
    
    def __init__(self, employees: List[str]):
        """Initialize with list of known employee names.
        
        Args:
            employees: List of employee names (e.g., from ApiKey table)
        """
        self.employees = sorted(employees, key=len, reverse=True)
        self.employees_lower = [e.lower() for e in self.employees]
        self._system_prefixes = ("AI-", "Основной", "Системный")
    
    def _is_system_employee(self, name: str) -> bool:
        """Check if employee is a system account (not real user)."""
        return any(name.startswith(p) for p in self._system_prefixes)
    
    def _exact_match(self, query: str) -> Optional[EmployeeMatch]:
        """Exact string match (case-insensitive).
        
        Example: "Иванов" matches "Иванов А.В." if exact in query
        """
        query_lower = query.lower()
        for emp, emp_lower in zip(self.employees, self.employees_lower):
            if emp_lower == query_lower:
                return EmployeeMatch(
                    name=emp,
                    confidence=1.0,
                    method='exact'
                )
        return None
    
    def _fuzzy_match(self, query: str, min_ratio: float = 0.75) -> List[EmployeeMatch]:
        """Fuzzy match using difflib.SequenceMatcher.
        
        Handles typos, partial names, transpositions.
        Returns sorted by confidence descending.
        
        Args:
            query: Query text
            min_ratio: Minimum matching ratio (0.0-1.0)
        
        Returns:
            List of matches sorted by confidence
        """
        query_lower = query.lower()
        matches = []
        
        for emp, emp_lower in zip(self.employees, self.employees_lower):
            # Compare full names
            ratio = difflib.SequenceMatcher(None, query_lower, emp_lower).ratio()
            if ratio >= min_ratio:
                matches.append(EmployeeMatch(
                    name=emp,
                    confidence=ratio,
                    method='fuzzy'
                ))
        
        # Sort by confidence descending
        matches.sort(key=lambda m: m.confidence, reverse=True)
        return matches
    
    def _partial_match(self, query: str) -> List[EmployeeMatch]:
        """Match by individual words/parts of ФИО.
        
        Example: "Кузьмицкий" in query → matches "Кузьмицкий Александр"
        
        Lower confidence than fuzzy match.
        Returns sorted by word count match (longer = better).
        """
        query_lower = query.lower()
        query_parts = query_lower.split()
        matches = []
        
        for emp, emp_lower in zip(self.employees, self.employees_lower):
            emp_parts = emp_lower.split()
            
            # Check if any query part matches any employee part
            for qpart in query_parts:
                if len(qpart) < 3:  # Skip too short matches
                    continue
                
                for epart in emp_parts:
                    if qpart in epart or epart in qpart:
                        # Confidence based on part length ratio
                        confidence = len(qpart) / max(len(qpart), len(epart))
                        # Boost confidence if full part matched (not substring)
                        if qpart == epart:
                            confidence = 0.95
                        
                        matches.append(EmployeeMatch(
                            name=emp,
                            confidence=confidence,
                            method='partial'
                        ))
                        break
        
        # Dedup by name, keep highest confidence
        seen = {}
        for m in matches:
            if m.name not in seen or m.confidence > seen[m.name].confidence:
                seen[m.name] = m
        
        return sorted(seen.values(), key=lambda m: m.confidence, reverse=True)
    
    def resolve(
        self, 
        query: str, 
        exclude_system: bool = True,
        top_k: int = None
    ) -> List[EmployeeMatch]:
        """Resolve employee name(s) from query.
        
        Tries in order: exact → fuzzy → partial.
        Returns best matches with confidence scores.
        
        Args:
            query: Natural language query
            exclude_system: If True, skip AI-*, Основной, Системный accounts
            top_k: Return only top K results (None = all)
        
        Returns:
            List[EmployeeMatch] sorted by confidence descending
        """
        if not query or not query.strip():
            return []
        
        # Try exact match first
        exact = self._exact_match(query)
        if exact:
            results = [exact]
        else:
            # Try fuzzy match (high threshold)
            fuzzy = self._fuzzy_match(query, min_ratio=0.75)
            if fuzzy:
                results = fuzzy
            else:
                # Try partial match (lower confidence)
                results = self._partial_match(query)
        
        # Filter system employees if requested
        if exclude_system:
            results = [m for m in results if not self._is_system_employee(m.name)]
        
        # Return top K
        if top_k:
            results = results[:top_k]
        
        return results
    
    def resolve_single(
        self, 
        query: str, 
        min_confidence: float = 0.6
    ) -> Optional[str]:
        """Convenience: return single best match or None.
        
        Args:
            query: Natural language query
            min_confidence: Minimum confidence threshold
        
        Returns:
            Best matching employee name, or None
        """
        matches = self.resolve(query, exclude_system=True, top_k=1)
        if matches and matches[0].confidence >= min_confidence:
            return matches[0].name
        return None
    
    def resolve_many(
        self, 
        query: str, 
        min_confidence: float = 0.6
    ) -> List[str]:
        """Convenience: return all matches above confidence threshold.
        
        Args:
            query: Natural language query
            min_confidence: Minimum confidence threshold
        
        Returns:
            List of matching employee names
        """
        matches = self.resolve(query, exclude_system=True)
        return [m.name for m in matches if m.confidence >= min_confidence]


def _create_resolver_from_db() -> EmployeeResolver:
    """Factory: create EmployeeResolver from DB."""
    try:
        from config import get_session, ApiKey
        session = get_session()
        try:
            keys = session.query(ApiKey.name).filter(
                ApiKey.name.isnot(None), 
                ApiKey.name != ""
            ).all()
            employees = [k[0] for k in keys]
            print(f"[RAG][EmployeeResolver] loaded {len(employees)} employees from DB")
            return EmployeeResolver(employees)
        finally:
            session.close()
    except Exception as e:
        print(f"[RAG][EmployeeResolver] failed to load from DB: {e}")
        # Fallback to hardcoded list (12 known employees)
        return EmployeeResolver([
            "Филатов Дмитрий",
            "Хромов Артём",
            "Кузьмицкий Александр",
            "Боровиков Алексей",
            "Ягудин Александр",
            "Иванков Николай",
            "Радько Кирилл",
            "Сапач Андрей",
            "Наточий Александр",
            "Тесленко Павел",
        ])
