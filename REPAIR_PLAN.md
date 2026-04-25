# POLZA.AI RAG System Repair Plan — Phase-7 "Hardening & Refactoring"

**Date Created:** 2026-04-26  
**Status:** PLANNING  
**Overall Risk Reduction:** 14 issues (5 CRITICAL, 8 HIGH, 1 MEDIUM) → **0** by end of Phase-7

---

## Wave-1: URGENT (Quick-Fixes for CRITICAL Issues)
**Timeline:** 2 days  
**Target:** Disable/mitigate max damage without major refactoring

### 1.1 [CRITICAL] Disable Auto-Dossier Fallback
- **File:** `rag/search.py:751-763`
- **Fix:** Remove auto-dossier fallback completely OR gate behind RBAC check
- **Verification:** Query with 0 semantic results → "По вашему запросу не найдено данных." (no auto-expansion)
- **Risk Reduction:** 80% (PII leakage via unintended dossier)

### 1.2 [CRITICAL] Raise RAG_MIN_SCORE from 0.15 to 0.4
- **File:** `config.py:68`
- **Fix:** Change default from 0.15 → 0.4 (cosine threshold)
- **Verification:** Test with typical query → sources should be < 10 (vs current 20-30)
- **Risk Reduction:** 60% (noise reduction, token efficiency)

### 1.3 [CRITICAL] Add Prompt Injection Guardrails
- **File:** `rag/chat.py:104-119` (context building)
- **Fix:** Wrap user_content + source snippets in XML tags + add basic validation
- **Verification:** Inject "FORGET..." in query → system detects and rejects
- **Risk Reduction:** 75% (prompt injection attack surface)

---

## Wave-2: Phase-7 Main Refactoring
**Timeline:** 2 weeks  
**Target:** Complete structural fixes + full test coverage

### 2.1 Module Split & SOLID Refactoring
**Decompose `rag/search.py` (798 lines) into:**

#### M-EMPLOYEE-RESOLVER (new module)
- **File:** `rag/employee_resolver.py` (new)
- **Components:**
  - `class EmployeeResolver`: fuzzy string matching + exact match
  - `resolve(query: str) -> List[EmployeeMatch]`: with confidence scores
- **Tests:** 50+ unit tests (Russian names, collisions, typos, etc.)
- **Replaces:** `_detect_employee_filter()` + `_get_employee_names()`

#### M-RAG-RETRIEVER (new module)
- **File:** `rag/retriever.py` (new)
- **Components:**
  - `class QueryMode(Enum)`: SEARCH, DOSSIER, EMPLOYEE_LIST, GLOBAL_AGG
  - `QueryClassifier`: detect mode from keywords
  - `retrieve(query: str) -> (mode, sources)`
- **Tests:** Integration tests for all 4 modes

#### M-RAG-CONTEXT-BUILDER (new module)
- **File:** `rag/context_builder.py` (new)
- **Components:**
  - `ContextBuilder(mode: QueryMode)`: strategy pattern
  - `build() -> str`: return formatted context block
- **Tests:** Unit tests for each context type

#### M-RAG-GUARDRAILS (new module)
- **File:** `rag/guardrails.py` (new)
- **Components:**
  - `def _escape_xml(text: str) -> str`
  - `def _validate_injection(text: str) -> bool`
  - `def _redact_pii(text: str) -> str`
- **Tests:** Adversarial test suite with injection attempts

### 2.2 Core Fixes by Issue #

| Issue | Fix | Est. Days |
|-------|-----|-----------|
| #1 Substring matching | M-EMPLOYEE-RESOLVER (fuzzy + exact) | 2 |
| #2 Auto-dossier | Delete fallback code | 0.5 |
| #3 RAG_MIN_SCORE | Change 0.15→0.4 | 0.5 |
| #4 Global lock | Per-session lock + asyncio | 1 |
| #5 Prompt injection | M-RAG-GUARDRAILS (XML + validation) | 2 |
| #6 Follow-up accum. | Dedup + history compression | 1 |
| #7 Cache invalidation | TTL cache with event | 1 |
| #8 Ollama timeout | Exponential backoff + fallback | 1 |
| #9 Multiple names | Return list + disambiguation | 1.5 |
| #10 Memory leak | Redis sessions with TTL | 2 |
| #11 Qdrant thread-safety | Connection pool wrapper | 1 |
| #12 Context explosion | Compression + prioritization | 1.5 |
| #13 Schema mismatch | Strict validation + migration | 1 |
| #14 Frontend XSS | HTML escape in DOM render | 0.5 |

### 2.3 Testing Strategy
**Total: 150+ unit tests + 30 integration tests + adversarial suite**

- Unit tests: 50 (employee resolution) + 20 (retriever) + 15 (context) + 25 (guardrails) + 15 (embed) + 10 (qdrant) = 135
- Integration tests: 20 (RAG e2e) + 10 (concurrent sessions)
- Adversarial tests: 10 (prompt injection variants)

---

## Execution Timeline

**Week 1 (URGENT):**
- Implement Wave-1 fixes (auto-dossier, RAG_MIN_SCORE, guardrails)
- Deploy to staging, manual testing
- Deploy to production (hotfix)

**Week 2-3 (Phase-7 Refactoring):**
- Create M-EMPLOYEE-RESOLVER + tests
- Create M-RAG-RETRIEVER + tests
- Split search.py
- Update rag/chat.py (locks, follow-up dedup, sessions)
- Create 150+ unit + integration tests
- Deploy to staging, full integration testing

**Week 4 (Verification & Production):**
- Load testing (100 concurrent users)
- Code review (grok-critic)
- Update GRACE artifacts (development-plan.xml, verification-plan.xml, knowledge-graph.xml)
- Merge to main, deploy to production

---

## Success Criteria

- [x] 14 issues identified and documented
- [ ] All 5 CRITICAL issues fixed
- [ ] All 8 HIGH issues fixed
- [ ] 150+ passing unit tests
- [ ] 30+ passing integration tests
- [ ] Load test: 100 concurrent users, <5% error rate
- [ ] grok-critic score: 8/10+

---

## GRACE Artifacts to Update

1. **development-plan.xml:** Add Phase-7, new modules, update STATUS to "done"
2. **verification-plan.xml:** Add VF-015+ for RAG-specific flows
3. **knowledge-graph.xml:** Add new module nodes and dependencies
4. **operational-packets.xml:** Wave-1 and Wave-2 deployment packets

