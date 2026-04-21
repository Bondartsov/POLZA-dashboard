# GRACE Framework - Project Engineering Protocol

## Keywords
polza-ai, dashboard, monitoring, api-analytics, ai-summarization, anthropic-haiku, prompt-caching, cost-control, flask, postgresql, spa, topic-grouping

## Annotation
Polza.AI Dashboard — веб-дашборд мониторинга генераций AI-моделей. Отслеживает расходы команды из 12 сотрудников, суммаризирует каждый промпт через Claude Haiku 4.5 (Anthropic native API) по явному клику в детальной модалке, кеширует LLM-результаты в PostgreSQL (100% экономия на повторах), группирует запросы по AI-темам и выявляет нецелевое использование через risk_flags (personal/sensitive/high_cost).

## Core Principles

### 1. Never Write Code Without a Contract
Before generating or editing any module, create or update its MODULE_CONTRACT with PURPOSE, SCOPE, INPUTS, and OUTPUTS. The contract is the source of truth. Code implements the contract, not the other way around.

### 2. Semantic Markup Is Load-Bearing Structure
Markers like `// START_BLOCK_<NAME>` and `// END_BLOCK_<NAME>` are navigation anchors, not documentation. They must be:
- uniquely named
- paired
- proportionally sized so one block fits inside an LLM working window

### 3. Knowledge Graph Is Always Current
`docs/knowledge-graph.xml` is the project map. When you add a module, move a module, rename exports, or add dependencies, update the graph so future agents can navigate deterministically.

### 4. Verification Is a FirstClass Artifact
Testing, traces, and log anchors are designed before large execution waves. `docs/verification-plan.xml` is part of the architecture, not an afterthought. Logs are evidence. Tests are executable contracts.

### 5. TopDown Synthesis
Code generation follows:
`RequirementsAnalysis -> TechnologyStack -> DevelopmentPlan -> VerificationPlan -> Code + Tests`

Never jump straight to code when requirements, architecture, or verification intent are still unclear.

### 6. Governed Autonomy
Agents have freedom in HOW to implement, but not in WHAT to build. Contracts, plans, graph references, and verification requirements define the allowed space.

## Semantic Markup Reference

### Module Level
```
// FILE: path/to/file.ext
// VERSION: 1.0.0
// START_MODULE_CONTRACT
//   PURPOSE: [What this module does - one sentence]
//   SCOPE: [What operations are included]
//   DEPENDS: [List of module dependencies]
//   LINKS: [Knowledge graph references]
// END_MODULE_CONTRACT
```

### Code Block Level
```
// START_BLOCK_<NAME>
// ... code ...
// END_BLOCK_<NAME>
```

## Logging and Trace Convention

All important logs must point back to semantic blocks:
```
print(f"[ModuleName][functionName][BLOCK_NAME] message")
```

Rules:
- prefer structured fields over prose-heavy log lines
- redact secrets and high-risk payloads
- treat missing log anchors on critical branches as a verification defect

## File Structure
```
docs/
  requirements.xml       - Product requirements and use cases
  technology.xml         - Stack decisions, tooling, observability, testing
  development-plan.xml   - Modules, phases, data flows, ownership, write scopes
  verification-plan.xml  - Test strategy, trace expectations, module and phase gates
  knowledge-graph.xml    - Project-wide navigation graph
  operational-packets.xml - Canonical packet, delta, and failure handoff templates
db.py                    - SQLAlchemy models
sync_worker.py           - Background sync from Polza.AI API
polza_dashboard.py       - Flask backend
static/
  index.html             - SPA shell
  style.css              - Styles
  app.js                 - Frontend SPA
```
